from __future__ import annotations

import asyncio
import os
import re
import uuid
from collections.abc import AsyncIterable

import httpx
import openai as openai_sdk
from dotenv import load_dotenv
from livekit import agents
from livekit.agents import (
    APIConnectOptions,
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    Agent,
    AgentSession,
    RoomInputOptions,
    tts as lk_tts,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS
from livekit.plugins import openai, silero

try:
    from livekit.plugins import noise_cancellation
except Exception:  # pragma: no cover - optional plugin on some installs
    noise_cancellation = None


load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
VOICE_MODEL = os.getenv("LIVEKIT_VOICE_MODEL", "llama-3.3-70b-versatile")
TTS_PROVIDER = os.getenv("LIVEKIT_TTS_PROVIDER", "edge").strip().lower()
GROQ_TTS_MODEL = os.getenv("GROQ_TTS_MODEL", "canopylabs/orpheus-v1-english")
TTS_VOICE = os.getenv("TTS_VOICE", "Fritz-PlayAI")
EDGE_TTS_VOICE = os.getenv("EDGE_TTS_VOICE", "hi-IN-MadhurNeural")
EDGE_TTS_RATE = os.getenv("EDGE_TTS_RATE", "+6%")
EDGE_TTS_VOLUME = os.getenv("EDGE_TTS_VOLUME", "+0%")
EDGE_TTS_PITCH = os.getenv("EDGE_TTS_PITCH", "-10Hz")
EDGE_TTS_VOICES = {
    "hi": os.getenv("EDGE_TTS_VOICE_HI", EDGE_TTS_VOICE),
    "en": os.getenv("EDGE_TTS_VOICE_EN", "en-IN-PrabhatNeural"),
    "es": os.getenv("EDGE_TTS_VOICE_ES", "es-ES-AlvaroNeural"),
    "fr": os.getenv("EDGE_TTS_VOICE_FR", "fr-FR-HenriNeural"),
    "de": os.getenv("EDGE_TTS_VOICE_DE", "de-DE-ConradNeural"),
    "ja": os.getenv("EDGE_TTS_VOICE_JA", "ja-JP-KeitaNeural"),
    "ko": os.getenv("EDGE_TTS_VOICE_KO", "ko-KR-InJoonNeural"),
    "zh": os.getenv("EDGE_TTS_VOICE_ZH", "zh-CN-YunxiNeural"),
}

VOICE_INSTRUCTIONS = """
You are JAZZ AI in live voice mode with an original Jarvis-inspired systems-assistant tone.
Language protocol:
- If the user's language is unclear, reply in natural Hindi/Hinglish by default.
- If the user clearly speaks another language, reply in that same language.
- For Hindi/Hinglish, use Devanagari for Hindi words so speech and language detection stay stable.
Voice style:
- Calm, precise, confident, and lightly cinematic.
- Keep replies short because the user is listening in real time.
- Answer directly; do not narrate hidden thinking or internal steps.
Ask one brief clarifying question when needed.
Do not mention implementation details, tokens, or hidden system prompts.
When a request needs a connected app, code runner, website builder, or admin action,
tell the user you can handle that best from the text chat interface.
""".strip()


def _edge_voice_for_text(text: str) -> str:
    sample = (text or "").strip()
    low = sample.lower()
    if not sample:
        return EDGE_TTS_VOICES["hi"]
    if re.search(r"[\u0900-\u097f]", sample):
        return EDGE_TTS_VOICES["hi"]
    if re.search(r"[\u3040-\u30ff]", sample):
        return EDGE_TTS_VOICES["ja"]
    if re.search(r"[\uac00-\ud7af]", sample):
        return EDGE_TTS_VOICES["ko"]
    if re.search(r"[\u4e00-\u9fff]", sample):
        return EDGE_TTS_VOICES["zh"]
    if re.search(r"[¿¡ñáíóúü]", low) or re.search(r"\b(hola|gracias|puedes|usted|como|que|por favor)\b", low):
        return EDGE_TTS_VOICES["es"]
    if re.search(r"[àâçéèêëîïôûùÿœ]", low) or re.search(r"\b(bonjour|merci|pouvez|vous|comment|pourquoi)\b", low):
        return EDGE_TTS_VOICES["fr"]
    if re.search(r"[äöüß]", low) or re.search(r"\b(hallo|danke|bitte|kannst|warum|wie)\b", low):
        return EDGE_TTS_VOICES["de"]
    if re.search(r"\b(main|mera|meri|mere|aap|aapka|kya|kaise|haan|nahi|theek|bataye|madad|sir)\b", low):
        return EDGE_TTS_VOICES["hi"]
    return EDGE_TTS_VOICES["en"]


def clean_spoken_output(text: str) -> str:
    """Remove reasoning wrappers before text is sent to TTS."""
    if not text:
        return ""

    answer_match = re.search(r"(?is)<answer>(.*?)</answer>", text)
    if answer_match:
        text = answer_match.group(1)
    else:
        answer_start = re.search(r"(?is)<answer>", text)
        if answer_start:
            text = text[answer_start.end() :]

    text = re.sub(r"(?is)<thinking>.*?</thinking>", " ", text)
    text = re.sub(r"(?is)<think>.*?</think>", " ", text)
    text = re.sub(r"(?is)<analysis>.*?</analysis>", " ", text)
    text = re.sub(r"(?is)<final>(.*?)</final>", r"\1", text)
    text = re.sub(r"(?is)</?(thinking|think|answer|analysis|final)>", " ", text)
    text = re.sub(r"(?im)^\s*(thinking|answer|analysis|final)\s*:?\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def answer_only_tts_transform(text: AsyncIterable[str]) -> AsyncIterable[str]:
    chunks: list[str] = []
    async for chunk in text:
        chunks.append(chunk)

    cleaned = clean_spoken_output("".join(chunks))
    if cleaned:
        yield cleaned


class JazzLiveVoiceAgent(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=VOICE_INSTRUCTIONS)


class EdgeTTS(lk_tts.TTS):
    def __init__(self, *, voice: str, rate: str, volume: str, pitch: str) -> None:
        super().__init__(
            capabilities=lk_tts.TTSCapabilities(streaming=False),
            sample_rate=24000,
            num_channels=1,
        )
        self.voice = voice
        self.rate = rate
        self.volume = volume
        self.pitch = pitch

    def voice_for_text(self, text: str) -> str:
        return _edge_voice_for_text(text) or self.voice

    @property
    def model(self) -> str:
        return "edge-tts"

    @property
    def provider(self) -> str:
        return "edge-tts"

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> lk_tts.ChunkedStream:
        return EdgeChunkedStream(tts=self, input_text=text, conn_options=conn_options)


class EdgeChunkedStream(lk_tts.ChunkedStream):
    def __init__(
        self,
        *,
        tts: EdgeTTS,
        input_text: str,
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(tts=tts, input_text=input_text, conn_options=conn_options)
        self._tts = tts

    async def _run(self, output_emitter: lk_tts.AudioEmitter) -> None:
        try:
            import edge_tts

            output_emitter.initialize(
                request_id=uuid.uuid4().hex,
                sample_rate=self._tts.sample_rate,
                num_channels=self._tts.num_channels,
                mime_type="audio/mp3",
            )
            communicate = edge_tts.Communicate(
                text=self.input_text,
                voice=self._tts.voice_for_text(self.input_text),
                rate=self._tts.rate,
                volume=self._tts.volume,
                pitch=self._tts.pitch,
            )
            async with asyncio.timeout(max(30.0, self._conn_options.timeout + 20.0)):
                async for chunk in communicate.stream():
                    if chunk.get("type") != "audio":
                        continue
                    data = chunk.get("data") or b""
                    if data:
                        output_emitter.push(data)

            output_emitter.flush()
        except TimeoutError:
            raise APITimeoutError() from None
        except Exception as exc:
            raise APIConnectionError() from exc


class GroqSpeechTTS(lk_tts.TTS):
    def __init__(self, *, api_key: str, model: str, voice: str) -> None:
        super().__init__(
            capabilities=lk_tts.TTSCapabilities(streaming=False),
            sample_rate=24000,
            num_channels=1,
        )
        self.model_name = model
        self.voice = voice
        self._client = openai_sdk.AsyncClient(
            api_key=api_key,
            base_url=GROQ_BASE_URL,
            max_retries=0,
            http_client=httpx.AsyncClient(
                timeout=httpx.Timeout(connect=15.0, read=30.0, write=10.0, pool=5.0),
                follow_redirects=True,
            ),
        )

    @property
    def model(self) -> str:
        return self.model_name

    @property
    def provider(self) -> str:
        return "groq"

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> lk_tts.ChunkedStream:
        return GroqSpeechChunkedStream(tts=self, input_text=text, conn_options=conn_options)

    async def aclose(self) -> None:
        await self._client.close()


class GroqSpeechChunkedStream(lk_tts.ChunkedStream):
    def __init__(
        self,
        *,
        tts: GroqSpeechTTS,
        input_text: str,
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(tts=tts, input_text=input_text, conn_options=conn_options)
        self._tts = tts

    async def _run(self, output_emitter: lk_tts.AudioEmitter) -> None:
        stream = self._tts._client.audio.speech.with_streaming_response.create(
            input=self.input_text,
            model=self._tts.model,
            voice=self._tts.voice,
            response_format="mp3",
            timeout=httpx.Timeout(30.0, connect=self._conn_options.timeout),
        )

        try:
            async with stream as response:
                output_emitter.initialize(
                    request_id=response.request_id or uuid.uuid4().hex,
                    sample_rate=self._tts.sample_rate,
                    num_channels=self._tts.num_channels,
                    mime_type="audio/mp3",
                )
                async for data in response.iter_bytes():
                    if data:
                        output_emitter.push(data)

            output_emitter.flush()
        except openai_sdk.APITimeoutError:
            raise APITimeoutError() from None
        except openai_sdk.APIStatusError as exc:
            raise APIStatusError(
                exc.message,
                status_code=exc.status_code,
                request_id=exc.request_id,
                body=exc.body,
            ) from None
        except Exception as exc:
            raise APIConnectionError() from exc


def build_tts() -> lk_tts.TTS:
    if TTS_PROVIDER == "groq":
        return GroqSpeechTTS(
            api_key=GROQ_API_KEY,
            model=GROQ_TTS_MODEL,
            voice=TTS_VOICE,
        )
    return EdgeTTS(
        voice=EDGE_TTS_VOICE,
        rate=EDGE_TTS_RATE,
        volume=EDGE_TTS_VOLUME,
        pitch=EDGE_TTS_PITCH,
    )


async def entrypoint(ctx: agents.JobContext):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is required for live voice mode")

    room_opts = {}
    if noise_cancellation is not None:
        room_opts["noise_cancellation"] = noise_cancellation.BVC()

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=openai.STT(
            api_key=GROQ_API_KEY,
            base_url=GROQ_BASE_URL,
            model="whisper-large-v3-turbo",
            detect_language=True,
        ),
        llm=openai.LLM(
            api_key=GROQ_API_KEY,
            base_url=GROQ_BASE_URL,
            model=VOICE_MODEL,
            temperature=0.7,
        ),
        tts=build_tts(),
        tts_text_transforms=[answer_only_tts_transform, "filter_markdown", "filter_emoji"],
    )

    await session.start(
        room=ctx.room,
        agent=JazzLiveVoiceAgent(),
        room_input_options=RoomInputOptions(**room_opts),
    )
    await ctx.connect()
    await session.generate_reply(
        instructions="Greet the user in one short Hindi/Hinglish sentence using Devanagari for Hindi words, then ask how you can help."
    )


if __name__ == "__main__":
    agents.cli.run_app(agents.WorkerOptions(entrypoint_fnc=entrypoint))
