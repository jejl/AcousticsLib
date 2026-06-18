"""Flask-Login wiring shared across AstroAcoustics Dash applications."""
from flask import Flask
from flask_login import LoginManager

from .user import AuthUser


def setup_login_manager(server: Flask, url_prefix: str = "") -> LoginManager:
    """Initialise Flask-Login on *server* and register the user loader.

    Args:
        server:     The Flask server backing the Dash app.
        url_prefix: Path prefix the app is mounted at ("" in dev, e.g.
                    "/soundclass" in production).  Used to build ``login_view``.

    Returns:
        The configured :class:`~flask_login.LoginManager`.
    """
    login_manager = LoginManager()
    login_manager.init_app(server)
    login_manager.login_view = f"{url_prefix}/login"

    @login_manager.user_loader
    def load_user(user_id: str):
        # Imported lazily so the DB layer is only touched when a request needs it.
        from calltrackerslib.database.repositories.users import UserRepository
        row = UserRepository.get_by_id(int(user_id))
        if row:
            return AuthUser(dict(row))
        return None

    return login_manager
