"""The one place that knows what an LLM provider is.

Provider knowledge used to live in five places — the BYOK allowlist, the
credential verifier, the price-sheet mapping, the display names, and the judge's
own tuple — and every one of them had to be edited to add a provider. Predictably
they drifted: a key could be stored for a provider the judge refused to route,
and a model could be priced under a name the credential store had never heard of.

Everything now derives from :data:`PROVIDERS`. Adding a provider is one entry.

Naming
------
Three names exist for the same thing and they are deliberately not merged:

  ``id``            what Fluiq stores and the API accepts ("xai")
  ``price_name``    what the ``model_prices`` sheet calls it ("xAI") — that
                    table is loaded from an external source and we do not get
                    to choose its spelling
  ``label``         what a person reads ("xAI (Grok)")

``polygate`` is the transport for every one of these, so its own aliases
("grok", "kimi") are accepted on input and normalised to ``id``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Provider:
    id: str
    label: str
    #: Name in the model_prices sheet; None when we have no pricing for it, in
    #: which case cost is reported as unknown rather than guessed at zero.
    price_name: Optional[str]
    #: Environment variable polygate falls back to for a platform-managed key.
    env_var: str
    #: Model-id prefixes that identify this provider, for routing a bare model
    #: name. Ordered longest-first at lookup so "gpt-oss" can't shadow "gpt".
    prefixes: Tuple[str, ...] = ()
    #: Cheapest authenticated GET that proves a key works without spending
    #: tokens. None means the key is accepted without a live check.
    verify: Optional[Tuple[str, Dict[str, str]]] = None
    #: Default judge model when this provider is chosen without one.
    default_model: Optional[str] = None
    #: False for providers that hold keys but cannot serve a judge/task call
    #: through polygate yet.
    routable: bool = True
    #: Extra names accepted on input, normalised to ``id``.
    aliases: Tuple[str, ...] = ()
    #: True when the provider cannot be called without an endpoint the customer
    #: supplies — its URL contains their own resource, workspace, account or
    #: project. The key form must collect it, because there is nothing to
    #: default to and a missing one fails at call time rather than at save time.
    needs_endpoint: bool = False


PROVIDERS: List[Provider] = [
    Provider(
        id="openai", label="OpenAI", price_name="OpenAI", env_var="OPENAI_API_KEY",
        prefixes=("gpt", "o1", "o3", "o4", "chatgpt"),
        verify=("https://api.openai.com/v1/models", {}),
        default_model="gpt-4o-mini", aliases=("gpt",),
    ),
    Provider(
        id="anthropic", label="Anthropic", price_name="Anthropic",
        env_var="ANTHROPIC_API_KEY", prefixes=("claude",),
        verify=("https://api.anthropic.com/v1/models", {"anthropic-version": "2023-06-01"}),
        default_model="claude-haiku-4-5-20251001", aliases=("claude",),
    ),
    Provider(
        id="gemini", label="Google Gemini", price_name="Google",
        env_var="GEMINI_API_KEY", prefixes=("gemini",),
        verify=("https://generativelanguage.googleapis.com/v1beta/models", {}),
        default_model="gemini-2.5-flash", aliases=("google",),
    ),
    Provider(
        id="mistral", label="Mistral", price_name="Mistral",
        env_var="MISTRAL_API_KEY", prefixes=("mistral", "magistral", "codestral", "ministral"),
        verify=("https://api.mistral.ai/v1/models", {}),
        default_model="mistral-small-latest",
    ),
    Provider(
        id="groq", label="Groq", price_name="Groq", env_var="GROQ_API_KEY",
        prefixes=("groq/",),
        verify=("https://api.groq.com/openai/v1/models", {}),
        default_model="llama-3.3-70b-versatile",
    ),
    Provider(
        id="together", label="Together AI", price_name="Together",
        env_var="TOGETHER_API_KEY", prefixes=("together/",),
        verify=("https://api.together.xyz/v1/models", {}),
        aliases=("togetherai",),
    ),
    Provider(
        id="fireworks", label="Fireworks", price_name="Fireworks",
        env_var="FIREWORKS_API_KEY", prefixes=("accounts/fireworks",),
        verify=("https://api.fireworks.ai/inference/v1/models", {}),
    ),
    Provider(
        id="perplexity", label="Perplexity", price_name="Perplexity",
        env_var="PERPLEXITY_API_KEY", prefixes=("sonar",),
        # Perplexity has no unauthenticated models route; the key is checked on
        # first real use rather than by a call that would itself cost tokens.
        verify=None, aliases=("sonar", "pplx"),
    ),
    Provider(
        id="xai", label="xAI (Grok)", price_name="xAI", env_var="XAI_API_KEY",
        prefixes=("grok",),
        verify=("https://api.x.ai/v1/models", {}),
        default_model="grok-3-mini", aliases=("grok",),
    ),
    Provider(
        id="cerebras", label="Cerebras", price_name="Cerebras",
        env_var="CEREBRAS_API_KEY", prefixes=("cerebras/",),
        verify=("https://api.cerebras.ai/v1/models", {}),
    ),
    Provider(
        id="deepseek", label="DeepSeek", price_name="DeepSeek",
        env_var="DEEPSEEK_API_KEY", prefixes=("deepseek",),
        verify=("https://api.deepseek.com/models", {}),
        default_model="deepseek-chat",
    ),
    Provider(
        id="moonshot", label="Moonshot (Kimi)", price_name="Moonshot",
        env_var="MOONSHOT_API_KEY", prefixes=("kimi", "moonshot"),
        verify=("https://api.moonshot.ai/v1/models", {}),
        default_model="kimi-k2-0711-preview", aliases=("kimi",),
    ),
    Provider(
        id="zai", label="Z.AI (GLM)", price_name="Z.AI",
        env_var="ZAI_API_KEY", prefixes=("glm",),
        # The models route lives under the same /api/paas/v4 root as chat, not
        # the /v1 nearly every other host uses.
        verify=("https://api.z.ai/api/paas/v4/models", {}),
        default_model="glm-4.6",
        # "z.ai" is the brand spelling; "zhipu"/"bigmodel" are the company and
        # the mainland platform. All four reach the same adapter.
        aliases=("z.ai", "glm", "zhipu", "bigmodel"),
    ),
    # ── Clouds ───────────────────────────────────────────────────────────────
    # polygate now has real adapters for all five — Bedrock signs with SigV4 and
    # speaks Converse, Azure uses its own header plus a deployment path, Vertex
    # takes a short-lived OAuth token — but they are still routable=False here,
    # and the reason is Fluiq's, not polygate's.
    #
    # Every one of them needs something alongside the key that this product has
    # nowhere to put: a resource host, a workspace, an account id, a project, a
    # region. Until provider keys can carry an endpoint, marking these routable
    # would let a customer pick Bedrock as a judge and get a failure at call
    # time — the exact "runtime failure disguised as configuration" that
    # test_every_routable_provider_can_be_reached exists to prevent.
    #
    # Listing them anyway is deliberate: customers store these keys and expect
    # to see them.
    Provider(
        id="bedrock", label="AWS Bedrock", price_name=None,
        env_var="AWS_ACCESS_KEY_ID",
        # No unauthenticated models route, and a signed probe would need the
        # region we do not have yet. The key is checked on first real use.
        verify=None,
        default_model="anthropic.claude-3-5-sonnet-20241022-v2:0",
        aliases=("aws",), routable=False,
    ),
    Provider(
        id="azure_openai", label="Azure OpenAI", price_name="OpenAI",
        env_var="AZURE_OPENAI_API_KEY", verify=None,
        aliases=("azure",), needs_endpoint=True, routable=False,
    ),
    Provider(
        id="vertex", label="Google Vertex AI", price_name="Google",
        env_var="GOOGLE_ACCESS_TOKEN", verify=None,
        aliases=("vertexai",), needs_endpoint=True, routable=False,
    ),
    Provider(
        id="databricks", label="Databricks", price_name=None,
        env_var="DATABRICKS_TOKEN", verify=None, needs_endpoint=True, routable=False,
    ),
    Provider(
        id="cloudflare", label="Cloudflare Workers AI", price_name=None,
        env_var="CLOUDFLARE_API_TOKEN", verify=None,
        aliases=("workers-ai",), needs_endpoint=True, routable=False,
    ),

    # ── Gateways ─────────────────────────────────────────────────────────────
    # These route to other people's models, so a model id is usually
    # "vendor/model" and prefix-based routing cannot identify them — which is
    # why none of them declare prefixes.
    Provider(
        id="openrouter", label="OpenRouter", price_name=None,
        env_var="OPENROUTER_API_KEY",
        verify=("https://openrouter.ai/api/v1/models", {}),
        default_model="anthropic/claude-sonnet-4.5",
    ),
    Provider(
        id="vercel", label="Vercel AI Gateway", price_name=None,
        env_var="AI_GATEWAY_API_KEY", verify=None, aliases=("ai-gateway",),
    ),
    Provider(
        id="baseten", label="Baseten", price_name=None,
        env_var="BASETEN_API_KEY",
        verify=("https://inference.baseten.co/v1/models", {}),
    ),
    Provider(
        id="deepinfra", label="DeepInfra", price_name=None,
        env_var="DEEPINFRA_API_KEY",
        verify=("https://api.deepinfra.com/v1/openai/models", {}),
    ),
    Provider(
        id="sambanova", label="SambaNova", price_name=None,
        env_var="SAMBANOVA_API_KEY",
        verify=("https://api.sambanova.ai/v1/models", {}),
    ),
    Provider(
        id="nebius", label="Nebius AI Studio", price_name=None,
        env_var="NEBIUS_API_KEY",
        verify=("https://api.studio.nebius.com/v1/models", {}),
    ),
    Provider(
        id="novita", label="Novita AI", price_name=None,
        env_var="NOVITA_API_KEY", verify=None,
    ),
    Provider(
        id="hyperbolic", label="Hyperbolic", price_name=None,
        env_var="HYPERBOLIC_API_KEY", verify=None,
    ),
]

BY_ID: Dict[str, Provider] = {p.id: p for p in PROVIDERS}

#: Every accepted input name → canonical id.
_ALIASES: Dict[str, str] = {
    **{p.id: p.id for p in PROVIDERS},
    **{alias: p.id for p in PROVIDERS for alias in p.aliases},
}

#: What a BYOK key may be stored against.
VALID_PROVIDERS = frozenset(BY_ID)

#: Providers a judge or task call can actually be routed to.
ROUTABLE_PROVIDERS = tuple(p.id for p in PROVIDERS if p.routable)

#: Providers whose URL contains something only the customer knows. The key form
#: must ask for it alongside the key; without it the failure lands at call time,
#: long after the person who could fix it has moved on.
NEEDS_ENDPOINT = frozenset(p.id for p in PROVIDERS if p.needs_endpoint)

#: id → model_prices spelling, for costing. Providers with no sheet map to None.
PRICE_PROVIDER: Dict[str, Optional[str]] = {p.id: p.price_name for p in PROVIDERS}

#: model_prices spelling (lowercased) → id, for the reverse lookup.
#
# Two providers can share a price sheet: Azure OpenAI serves OpenAI's models at
# OpenAI's prices. The *first* registration wins, so "OpenAI" resolves to the
# routable direct provider rather than the Azure deployment — a model priced
# under "OpenAI" must not be charged to an Azure credential that may not exist.
BYOK_FOR_PRICE: Dict[str, str] = {}
for _p in PROVIDERS:
    if _p.price_name:
        BYOK_FOR_PRICE.setdefault(_p.price_name.lower(), _p.id)

PROVIDER_DISPLAY: Dict[str, str] = {p.id: p.label for p in PROVIDERS}

VERIFY_ENDPOINTS: Dict[str, Tuple[str, Dict[str, str]]] = {
    p.id: p.verify for p in PROVIDERS if p.verify
}

DEFAULT_MODELS: Dict[str, str] = {
    p.id: p.default_model for p in PROVIDERS if p.default_model
}

# Longest prefix first so a specific match wins over a generic one — otherwise
# "gpt" would claim a hypothetical "gpt-oss" served by another provider.
_PREFIX_INDEX: List[Tuple[str, str]] = sorted(
    ((prefix, p.id) for p in PROVIDERS for prefix in p.prefixes),
    key=lambda item: len(item[0]),
    reverse=True,
)


def normalize(name: str) -> Optional[str]:
    """Canonical id for any accepted spelling, or None."""
    return _ALIASES.get((name or "").strip().lower())


def provider_for_model(model: str) -> Optional[str]:
    """Resolve the provider that serves a bare model id.

    Prefix-based so a newly-priced model works without a code change; the
    ``model_prices`` row is the authority on which models exist, and this is only
    the mapping to the key that pays for the call.
    """
    m = (model or "").strip().lower()
    if not m:
        return None
    for prefix, provider_id in _PREFIX_INDEX:
        if m.startswith(prefix):
            return provider_id
    return None


def label(provider_id: str) -> str:
    return PROVIDER_DISPLAY.get(provider_id, provider_id)


__all__ = [
    "BYOK_FOR_PRICE",
    "BY_ID",
    "DEFAULT_MODELS",
    "PRICE_PROVIDER",
    "PROVIDERS",
    "PROVIDER_DISPLAY",
    "NEEDS_ENDPOINT",
    "ROUTABLE_PROVIDERS",
    "VALID_PROVIDERS",
    "VERIFY_ENDPOINTS",
    "Provider",
    "label",
    "normalize",
    "provider_for_model",
]
