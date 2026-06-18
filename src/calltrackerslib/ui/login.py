"""Shared login page — layout + callback factory.

Each app registers its own Dash page (so ``use_pages`` discovers it) and reuses
this layout and callback::

    import dash
    from calltrackerslib.ui import login_layout, register_login_callback
    from app import URL_PREFIX
    import db

    dash.register_page(__name__, path="/login", title="Login — SoundClass")
    layout = login_layout("SoundClass", url_prefix=URL_PREFIX)
    register_login_callback(db.authenticate_user, redirect_path="/classify",
                            url_prefix=URL_PREFIX)

The ``authenticate`` callable takes ``(username, password)`` and returns one of:

    None            — invalid credentials / unknown user
    "disabled"      — account exists but is disabled
    dict            — success; must contain at least an ``id`` key

On success the full user row is reloaded via ``UserRepository.get_by_id`` so the
session user always carries ``full_name`` regardless of how slim the dict is.
"""
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html
from flask_login import login_user

from .user import AuthUser


def login_layout(app_title: str, url_prefix: str = "", show_forgot: bool = True):
    """Return the centred login card layout for *app_title*."""
    footer = None
    if show_forgot:
        footer = dbc.CardFooter(
            dcc.Link(
                "Forgot your password?",
                href=f"{url_prefix}/forgot-password",
                className="text-muted small",
            )
        )

    card_children = [
        dbc.CardHeader(html.H4(app_title, className="mb-0 text-center")),
        dbc.CardBody([
            html.P("Sign in to continue.", className="text-muted text-center mb-4"),
            dbc.Label("Username"),
            dbc.Input(id="login-username", type="text", placeholder="Username",
                      className="mb-3", autofocus=True),
            dbc.Label("Password"),
            dbc.Input(id="login-password", type="password", placeholder="Password",
                      className="mb-2"),
            dbc.Checklist(
                options=[{"label": "Remember me", "value": 1}],
                value=[],
                id="login-remember",
                className="mb-3",
            ),
            dbc.Alert(id="login-error", color="danger", is_open=False, className="mb-3"),
            dbc.Button("Sign in", id="login-submit", color="primary", className="w-100"),
            dcc.Location(id="login-redirect", refresh=True),
        ]),
    ]
    if footer is not None:
        card_children.append(footer)

    card = dbc.Card(card_children, className="shadow-sm", style={"width": "420px"})

    return html.Div(
        dbc.Row(
            dbc.Col(card, width="auto"),
            justify="center",
            align="center",
            className="min-vh-100",
        ),
        className="bg-light",
    )


def register_login_callback(authenticate, redirect_path: str = "/", url_prefix: str = ""):
    """Register the login callback that authenticates and starts the session.

    Args:
        authenticate:  ``(username, password) -> None | "disabled" | dict``.
        redirect_path: App-local path to send the user to on success
                       (e.g. "/" or "/classify"). Prefixed with *url_prefix*.
        url_prefix:    Path prefix the app is mounted at.
    """

    @callback(
        Output("login-redirect", "pathname"),
        Output("login-error", "children"),
        Output("login-error", "is_open"),
        Input("login-submit", "n_clicks"),
        Input("login-username", "n_submit"),
        Input("login-password", "n_submit"),
        State("login-username", "value"),
        State("login-password", "value"),
        State("login-remember", "value"),
        prevent_initial_call=True,
    )
    def handle_login(_n_clicks, _u_submit, _p_submit, username, password, remember):
        if not username or not password:
            return dash.no_update, "Please enter both username and password.", True

        result = authenticate(username.strip(), password)

        if result is None:
            return dash.no_update, "Invalid username or password.", True
        if result == "disabled":
            return (dash.no_update,
                    "Your account has been disabled. Contact the administrator.", True)

        # Reload the full user row so AuthUser always carries full_name.
        from calltrackerslib.database.repositories.users import UserRepository
        full_user = UserRepository.get_by_id(result["id"])
        user_dict = dict(full_user) if full_user else dict(result)

        login_user(AuthUser(user_dict), remember=bool(remember))
        return f"{url_prefix}{redirect_path}", dash.no_update, False

    return handle_login
