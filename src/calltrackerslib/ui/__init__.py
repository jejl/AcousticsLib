"""Shared Dash/Flask UI building blocks for AstroAcoustics applications.

This subpackage requires the optional ``[ui]`` extra (Dash, dash-bootstrap-
components, flask-login).  It is intentionally NOT imported by
``calltrackerslib/__init__.py`` so that non-UI consumers (pipelines, scripts)
never pull in a web framework.

Public surface:
    AuthUser                        — Flask-Login user model
    setup_login_manager             — wire LoginManager + user_loader
    register_logout_route           — /logout Flask route
    login_layout / register_login_callback
    forgot_password_layout / register_forgot_password_callback
    reset_password_layout / register_reset_password_callback
    make_navbar                     — base navbar factory
"""
from .user import AuthUser
from .flask_login_setup import setup_login_manager
from .logout import register_logout_route
from .login import login_layout, register_login_callback
from .password_reset import (
    forgot_password_layout,
    register_forgot_password_callback,
    reset_password_layout,
    register_reset_password_callback,
)
from .navbar import make_navbar

__all__ = [
    "AuthUser",
    "setup_login_manager",
    "register_logout_route",
    "login_layout",
    "register_login_callback",
    "forgot_password_layout",
    "register_forgot_password_callback",
    "reset_password_layout",
    "register_reset_password_callback",
    "make_navbar",
]
