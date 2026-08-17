"""The single provider registry.

Provider knowledge used to live in five places and drifted: a key could be
stored for a provider the judge refused to route, and a model could be priced
under a name the credential store had never heard of. These pin the invariants
that make one registry actually one.
"""
from __future__ import annotations

import pytest

from shared.providers import (
    BY_ID,
    BYOK_FOR_PRICE,
    DEFAULT_MODELS,
    PRICE_PROVIDER,
    PROVIDER_DISPLAY,
    PROVIDERS,
    ROUTABLE_PROVIDERS,
    VALID_PROVIDERS,
    VERIFY_ENDPOINTS,
    label,
    normalize,
    provider_for_model,
)


# ── Registry integrity ───────────────────────────────────────────────────────

def test_ids_are_unique():
    ids = [p.id for p in PROVIDERS]
    assert len(ids) == len(set(ids))


def test_aliases_never_collide_with_an_id_of_another_provider():
    """An alias shadowing a different provider's id would silently reroute keys."""
    ids = {p.id for p in PROVIDERS}
    for provider in PROVIDERS:
        for alias in provider.aliases:
            assert alias not in (ids - {provider.id}), f"{alias} shadows another provider"


def test_aliases_are_globally_unique():
    seen: dict[str, str] = {}
    for provider in PROVIDERS:
        for alias in provider.aliases:
            assert alias not in seen, f"{alias} claimed by {seen.get(alias)} and {provider.id}"
            seen[alias] = provider.id


def test_every_derived_map_covers_the_registry():
    """A map missing an entry is how a provider becomes half-supported."""
    for provider in PROVIDERS:
        assert provider.id in PRICE_PROVIDER
        assert provider.id in PROVIDER_DISPLAY
        assert provider.id in VALID_PROVIDERS


def test_price_reverse_lookup_prefers_the_direct_provider():
    """Azure OpenAI serves OpenAI's models at OpenAI's prices, so both claim the
    "OpenAI" sheet. A model priced under it must resolve to the direct provider —
    charging it to an Azure credential the org may not have would just fail."""
    assert BYOK_FOR_PRICE["openai"] == "openai"


def test_every_price_name_resolves_to_a_real_provider():
    for price_name, provider_id in BYOK_FOR_PRICE.items():
        assert provider_id in BY_ID, price_name


def test_a_provider_without_pricing_reports_none_rather_than_zero():
    """Guessing zero would tell a customer a paid call was free."""
    assert PRICE_PROVIDER["bedrock"] is None


# ── Routability ──────────────────────────────────────────────────────────────

def test_key_holding_and_routable_are_different_sets():
    """The clouds accept keys but cannot yet be judged against.

    polygate has adapters for all of them now, so the blocker is on this side:
    each needs a resource host, workspace, account, project or region stored
    beside the key, and provider keys have nowhere to keep one. Until they do,
    routing a judge call there would fail at call time.
    """
    for name in ("bedrock", "azure_openai", "vertex", "databricks", "cloudflare"):
        assert name in VALID_PROVIDERS, f"{name} must still be storable"
        assert name not in ROUTABLE_PROVIDERS, (
            f"{name} cannot be routable until provider keys can carry an endpoint"
        )


def test_the_gateways_are_routable():
    """Gateways need nothing beyond a key — fixed host, OpenAI wire format — so
    they are usable the moment the key is saved."""
    for name in ("openrouter", "vercel", "baseten", "deepinfra",
                 "sambanova", "nebius", "novita", "hyperbolic"):
        assert name in ROUTABLE_PROVIDERS


def test_every_endpoint_provider_is_flagged_as_needing_one():
    """The key form reads this to decide whether to ask for an endpoint. A
    provider missing from it would collect a key that can never be used."""
    from shared.providers import NEEDS_ENDPOINT
    for name in ("azure_openai", "vertex", "databricks", "cloudflare"):
        assert name in NEEDS_ENDPOINT
    # Bedrock is the exception: it needs a region, which has a real default,
    # not an endpoint the customer must supply.
    assert "bedrock" not in NEEDS_ENDPOINT


def test_the_common_providers_are_routable():
    for name in ("openai", "anthropic", "gemini", "groq", "mistral", "xai", "deepseek"):
        assert name in ROUTABLE_PROVIDERS


def test_every_routable_provider_can_be_reached():
    """A provider the judge accepts but no transport serves is a runtime failure
    disguised as configuration."""
    from shared.completions import OPENAI_COMPATIBLE_BASES

    native = {"anthropic", "gemini"}
    for name in ROUTABLE_PROVIDERS:
        assert name in native or name in OPENAI_COMPATIBLE_BASES, f"{name} unreachable"


# ── Name normalisation ───────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "given,expected",
    [
        ("openai", "openai"), ("gpt", "openai"),
        ("claude", "anthropic"), ("google", "gemini"),
        ("grok", "xai"), ("kimi", "moonshot"),
        ("sonar", "perplexity"), ("pplx", "perplexity"),
        ("togetherai", "together"),
    ],
)
def test_popular_names_normalise(given, expected):
    assert normalize(given) == expected


def test_normalisation_ignores_case_and_padding():
    assert normalize("  XAI ") == "xai"


def test_an_unknown_name_is_none_rather_than_a_guess():
    assert normalize("nope") is None
    assert normalize("") is None
    assert normalize(None) is None  # type: ignore[arg-type]


# ── Model → provider routing ─────────────────────────────────────────────────

@pytest.mark.parametrize(
    "model,expected",
    [
        ("gpt-5-mini", "openai"),
        ("o3-mini", "openai"),
        ("claude-sonnet-4-6", "anthropic"),
        ("gemini-2.5-pro", "gemini"),
        ("mistral-large-latest", "mistral"),
        ("magistral-medium", "mistral"),
        ("grok-3", "xai"),
        ("sonar-pro", "perplexity"),
        ("deepseek-chat", "deepseek"),
        ("kimi-k2-0711-preview", "moonshot"),
    ],
)
def test_a_bare_model_routes_to_its_provider(model, expected):
    assert provider_for_model(model) == expected


def test_routing_is_case_insensitive():
    assert provider_for_model("GPT-5-Mini") == "openai"


def test_an_unknown_model_is_none():
    """None means "we can't tell", which the caller reports. A wrong guess would
    charge the customer's OpenAI key for someone else's model."""
    assert provider_for_model("llama-3-70b") is None
    assert provider_for_model("") is None


def test_the_longest_prefix_wins():
    """Otherwise a generic prefix claims a more specific provider's models."""
    from shared.providers import _PREFIX_INDEX

    lengths = [len(prefix) for prefix, _ in _PREFIX_INDEX]
    assert lengths == sorted(lengths, reverse=True)


# ── Verification ─────────────────────────────────────────────────────────────

def test_verify_endpoints_are_https_and_hardcoded():
    """These are fetched with a customer-supplied key; a templated host would be
    an SSRF."""
    for provider_id, (url, _headers) in VERIFY_ENDPOINTS.items():
        assert url.startswith("https://"), provider_id
        assert "{" not in url and "%" not in url, provider_id


def test_anthropic_verification_sends_its_version_header():
    """Anthropic rejects an unversioned request, which would read as a bad key."""
    _url, headers = VERIFY_ENDPOINTS["anthropic"]
    assert headers.get("anthropic-version")


def test_a_provider_without_a_verify_route_is_simply_absent():
    """Perplexity has no unauthenticated models route; checking with a call that
    costs tokens would be worse than checking on first real use."""
    assert "perplexity" not in VERIFY_ENDPOINTS
    assert "perplexity" in VALID_PROVIDERS


# ── Defaults and labels ──────────────────────────────────────────────────────

def test_default_judge_models_belong_to_their_provider():
    """Only checked for providers whose model names identify them.

    Groq, Together, Fireworks and Cerebras host *other people's* models —
    `llama-3.3-70b-versatile` names no host — so a bare model id cannot route to
    them, which is exactly why they need an explicit `provider:model` spec.
    Asserting otherwise would demand a routing rule that would then mis-claim
    every Llama model on every other host.

    The gateways and Bedrock are the same case, more so: an OpenRouter id is
    literally `anthropic/claude-sonnet-4.5`, and a Bedrock one is
    `anthropic.claude-3-5-sonnet-20241022-v2:0`. Routing either by prefix would
    hand every Anthropic call to the wrong provider.
    """
    multi_tenant = {
        "groq", "together", "fireworks", "cerebras", "bedrock",
        "openrouter", "vercel", "baseten", "deepinfra", "sambanova",
        "nebius", "novita", "hyperbolic",
    }
    checked = 0
    for provider_id, model in DEFAULT_MODELS.items():
        if provider_id in multi_tenant:
            continue
        assert provider_for_model(model) == provider_id, f"{model} is not a {provider_id} model"
        checked += 1
    assert checked >= 5, "the exemption list has swallowed the test"


def test_a_multi_tenant_host_still_has_a_default_or_none():
    """It may legitimately have neither; what it must not have is a default that
    silently routes elsewhere."""
    for provider_id in ("groq", "together", "fireworks", "cerebras"):
        model = DEFAULT_MODELS.get(provider_id)
        if model:
            routed = provider_for_model(model)
            assert routed in (None, provider_id), f"{model} routes to {routed}"


def test_every_provider_has_a_human_label():
    for provider in PROVIDERS:
        assert label(provider.id) == provider.label
        assert provider.label and provider.label != provider.id.upper()


def test_an_unknown_id_labels_as_itself_rather_than_blank():
    assert label("mystery") == "mystery"
