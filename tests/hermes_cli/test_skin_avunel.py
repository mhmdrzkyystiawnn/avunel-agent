"""Avunel skin: brand palette, monogram, DM Serif Display channel, and the
streaming-response branding path.

The skin engine is the one theme unit every surface reads, so these tests pin
the Avunel identity where it is actually consumed: the resolved SkinConfig, the
prompt_toolkit style overrides, and the response-box label used on the streaming
voice path (which used to hardcode "Hermes").
"""

from hermes_cli.skin_engine import (
    _BUILTIN_SKINS,
    get_active_skin,
    get_prompt_toolkit_style_overrides,
    list_skins,
    load_skin,
    set_active_skin,
)

# The exact brand anchors from the brief.
BURGUNDY = "#790D16"
WARM_IVORY = "#F5EFE1"
DUSTY_BLUE = "#AEC4D4"
# Burgundy lifts to a readable tint on a dark pole; the raw anchor stays in the overlay.
BURGUNDY_DARK = "#C45862"
DUSTY_BLUE_BORDER = "#5C6B78"


def test_avunel_is_a_builtin_and_listed():
    assert "avunel" in _BUILTIN_SKINS
    assert "avunel" in [s["name"] for s in list_skins()]


def test_avunel_wordmark_and_branding_are_lowercase():
    skin = load_skin("avunel")
    assert skin.get_branding("agent_name") == "avunel"
    assert skin.get_branding("welcome").startswith("avunel")
    assert skin.get_branding("response_label").strip() == "◆ avunel"
    assert skin.get_branding("prompt_symbol") == "❯"


def test_avunel_uses_the_brand_accent_and_neutral_text():
    skin = load_skin("avunel")
    # Burgundy is the signature accent — active states, not body text. On a dark
    # pole it lifts to a readable tint (the raw #790D16 is only 1.7:1 there).
    assert skin.get_color("banner_accent") == BURGUNDY_DARK
    assert skin.get_color("ui_accent") == BURGUNDY_DARK
    assert skin.get_color("status_bar_strong") == BURGUNDY_DARK
    assert skin.get_color("shell_dollar") == BURGUNDY_DARK
    # Warm Ivory carries the text; Dusty Blue stays the secondary/metadata tone.
    assert skin.get_color("banner_text") == WARM_IVORY
    assert skin.get_color("banner_title") == WARM_IVORY
    assert skin.get_color("banner_dim") == DUSTY_BLUE
    assert skin.get_color("session_label") == DUSTY_BLUE
    # The terminal background is never owned: no `background` key, and the status
    # bar keeps a dark fill (not a global panel).
    assert "background" not in skin.colors


def test_avunel_monogram_is_a_geometric_a_with_wordmark():
    skin = load_skin("avunel")
    hero = skin.banner_hero
    # The mark is the identity carrier on a terminal; it must render the A shape
    # and pair with the lowercase wordmark.
    assert "▗▄▄▖" in hero
    assert "avunel" in hero
    # Restrained: not a giant multi-line banner.
    assert len(hero.splitlines()) <= 8
    assert skin.banner_logo.startswith("[bold")


def test_avunel_dm_serif_display_only_on_graphical_wordmark():
    skin = load_skin("avunel")
    # DM Serif Display reaches the GUI as an injected stylesheet, scoped to the
    # display wordmark — never the terminal, never conversation/technical text.
    assert "DM+Serif+Display" in skin.custom_css
    assert ".wordmark" in skin.custom_css


def test_avunel_light_overlay_darkens_the_bright_brand_foregrounds():
    overlay = _BUILTIN_SKINS["avunel"]["light_colors"]
    # Warm Ivory / the burgundy tint invert to unreadable on a light terminal, so the
    # overlay must supply dark replacements; the raw Burgundy anchor reads there.
    assert overlay["banner_text"] != WARM_IVORY
    assert overlay["banner_accent"] == BURGUNDY
    assert overlay["status_bar_bg"] != "#171315"  # fills flip to light polarity


def test_avunel_style_overrides_drive_the_tui():
    set_active_skin("avunel")
    skin = get_active_skin()
    ov = get_prompt_toolkit_style_overrides()
    assert ov["prompt"] == skin.get_color("prompt")
    assert ov["status-bar"] == f"bg:{skin.get_color('status_bar_bg')} {WARM_IVORY}"
    assert ov["status-bar-strong"] == f"bg:{skin.get_color('status_bar_bg')} {BURGUNDY_DARK} bold"


def test_streaming_response_box_uses_the_skin_label():
    """The streaming voice path must read the skin, not a hardcoded 'Hermes'."""
    from hermes_cli.skin_engine import get_active_skin

    set_active_skin("avunel")
    # Mirrors the lookup the display_callback now performs.
    label = get_active_skin().get_branding("response_label", " ☤ Hermes ")
    assert label.strip() == "◆ avunel"
    assert "Hermes" not in label


def test_status_bar_glyph_is_skin_aware():
    """The status-bar marker before the model must follow the skin, defaulting to ☤."""
    from hermes_cli.cli_status_bar_mixin import _status_glyph

    set_active_skin("avunel")
    assert _status_glyph() == "◆"
    # A skin that sets no status_glyph keeps the Hermes caduceus.
    set_active_skin("default")
    assert _status_glyph() == "☤"
