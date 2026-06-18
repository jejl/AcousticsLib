"""Logout Flask route shared across AstroAcoustics Dash applications."""
from flask import Flask, redirect
from flask_login import logout_user


def register_logout_route(server: Flask, url_prefix: str = ""):
    """Register a ``{url_prefix}/logout`` route that clears the session.

    Args:
        server:     The Flask server backing the Dash app.
        url_prefix: Path prefix the app is mounted at ("" in dev, e.g.
                    "/soundclass" in production).

    Returns:
        The view function (mostly for testing).
    """

    @server.route(f"{url_prefix}/logout")
    def logout():
        logout_user()
        return redirect(f"{url_prefix}/login")

    return logout
