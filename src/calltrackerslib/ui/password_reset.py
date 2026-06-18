"""Shared forgot-password / reset-password Dash pages.

These wrap :class:`calltrackerslib.password_reset.PasswordResetService`.  Each
app registers thin Dash pages that reuse these layouts and callbacks::

    # pages/forgot_password.py
    import dash
    from calltrackerslib.ui import forgot_password_layout, register_forgot_password_callback
    from app import URL_PREFIX
    import config
    dash.register_page(__name__, path="/forgot-password", title="Forgot password — SoundClass")
    layout = forgot_password_layout("SoundClass", url_prefix=URL_PREFIX)
    register_forgot_password_callback(url_prefix=URL_PREFIX, app_base_url=config.APP_URL)

    # pages/reset_password.py
    import dash
    from calltrackerslib.ui import reset_password_layout, register_reset_password_callback
    from app import URL_PREFIX
    dash.register_page(__name__, path="/reset-password", title="Reset password — SoundClass")
    def layout(reset_token=None, **_):
        return reset_password_layout("SoundClass", url_prefix=URL_PREFIX, reset_token=reset_token)
    register_reset_password_callback(url_prefix=URL_PREFIX)

The email link built by ``request_reset`` is ``{app_base_url}/reset-password?reset_token=<token>``
(``request_reset`` appends ``?reset_token=<token>`` to the URL it is given).
"""
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
from flask import request

from calltrackerslib.password_reset import PasswordResetService
from calltrackerslib.password_validation import PASSWORD_REQUIREMENTS_TEXT


def _centred_card(card: dbc.Card):
    return html.Div(
        dbc.Row(
            dbc.Col(card, width="auto"),
            justify="center",
            align="center",
            className="min-vh-100",
        ),
        className="bg-light",
    )


# ── Forgot password ─────────────────────────────────────────────────────────

def forgot_password_layout(app_title: str, url_prefix: str = ""):
    """Return the 'request a reset link' layout."""
    card = dbc.Card([
        dbc.CardHeader(html.H4(app_title, className="mb-0 text-center")),
        dbc.CardBody([
            html.P("Enter your account email and we'll send a reset link.",
                   className="text-muted text-center mb-4"),
            dbc.Label("Email"),
            dbc.Input(id="fp-email", type="email", placeholder="you@example.com",
                      className="mb-3", autofocus=True),
            dbc.Alert(id="fp-alert", is_open=False, className="mb-3"),
            dbc.Button("Send reset link", id="fp-submit", color="primary", className="w-100"),
        ]),
        dbc.CardFooter(
            dcc.Link("Back to sign in", href=f"{url_prefix}/login",
                     className="text-muted small")
        ),
    ], className="shadow-sm", style={"width": "420px"})
    return _centred_card(card)


def register_forgot_password_callback(url_prefix: str = "", app_base_url: str | None = None):
    """Register the forgot-password submit callback.

    Args:
        url_prefix:   Path prefix the app is mounted at.
        app_base_url: External base URL of the app (e.g.
                      "https://pcsdata.cloud.edu.au/soundclass").  The reset link
                      becomes ``{app_base_url}/reset-password?reset_token=...``.
                      When None, it is derived from the incoming request as
                      ``{request.url_root}{url_prefix}/reset-password`` — fine for
                      dev but set it explicitly in production (behind a proxy the
                      request host may be the internal one).
    """

    @callback(
        Output("fp-alert", "children"),
        Output("fp-alert", "color"),
        Output("fp-alert", "is_open"),
        Input("fp-submit", "n_clicks"),
        Input("fp-email", "n_submit"),
        State("fp-email", "value"),
        prevent_initial_call=True,
    )
    def submit(_n_clicks, _n_submit, email):
        if not email or not email.strip():
            return "Please enter your email address.", "danger", True

        if app_base_url:
            reset_page_url = f"{app_base_url.rstrip('/')}/reset-password"
        else:
            reset_page_url = f"{request.url_root.rstrip('/')}{url_prefix}/reset-password"

        ok, msg = PasswordResetService.request_reset(email.strip(), reset_page_url)
        return msg, ("success" if ok else "danger"), True

    return submit


# ── Reset password ──────────────────────────────────────────────────────────

def reset_password_layout(app_title: str, url_prefix: str = "", reset_token: str | None = None):
    """Return the 'set a new password' layout.

    *reset_token* comes from the ``?reset_token=`` query param (Dash passes query
    params as keyword arguments to a page's ``layout`` function).
    """
    token = reset_token or ""

    if not token:
        body = [
            dbc.Alert(
                "This reset link is missing its token. Please use the link from "
                "your email, or request a new one.",
                color="danger", className="mb-3",
            ),
            dcc.Link("Request a new reset link", href=f"{url_prefix}/forgot-password",
                     className="text-muted small"),
        ]
    else:
        body = [
            html.P("Choose a new password for your account.",
                   className="text-muted text-center mb-3"),
            html.P(PASSWORD_REQUIREMENTS_TEXT, className="text-muted small mb-4"),
            dbc.Label("New password"),
            dbc.Input(id="rp-password", type="password", placeholder="New password",
                      className="mb-3", autofocus=True),
            dbc.Label("Confirm new password"),
            dbc.Input(id="rp-confirm", type="password", placeholder="Confirm new password",
                      className="mb-2"),
            dbc.Alert(id="rp-alert", is_open=False, className="mb-3"),
            dbc.Button("Reset password", id="rp-submit", color="primary", className="w-100"),
            dcc.Store(id="rp-token", data=token),
        ]

    card = dbc.Card([
        dbc.CardHeader(html.H4(app_title, className="mb-0 text-center")),
        dbc.CardBody(body),
        dbc.CardFooter(
            dcc.Link("Back to sign in", href=f"{url_prefix}/login",
                     className="text-muted small")
        ),
    ], className="shadow-sm", style={"width": "440px"})
    return _centred_card(card)


def register_reset_password_callback(url_prefix: str = ""):
    """Register the reset-password submit callback.

    On success the alert turns green and links to the login page; the token is
    single-use and consumed by ``complete_reset``.
    """

    @callback(
        Output("rp-alert", "children"),
        Output("rp-alert", "color"),
        Output("rp-alert", "is_open"),
        Output("rp-submit", "disabled"),
        Input("rp-submit", "n_clicks"),
        State("rp-password", "value"),
        State("rp-confirm", "value"),
        State("rp-token", "data"),
        prevent_initial_call=True,
    )
    def submit(_n_clicks, password, confirm, token):
        if not token:
            return "This reset link is invalid or has expired.", "danger", True, False
        if not password or not confirm:
            return "Please enter and confirm your new password.", "danger", True, False
        if password != confirm:
            return "Passwords do not match.", "danger", True, False

        ok, msg = PasswordResetService.complete_reset(token, password)
        if ok:
            login_link = dcc.Link("Go to sign in", href=f"{url_prefix}/login")
            return [html.Span(msg + " "), login_link], "success", True, True
        return msg, "danger", True, False

    return submit
