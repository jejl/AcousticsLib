"""Flask-Login user model shared across AstroAcoustics Dash applications."""


class AuthUser:
    """Minimal Flask-Login user object wrapping a CallTrackers user dict.

    The wrapped dict is expected to contain at least ``id`` and ``username``.
    ``full_name`` falls back to ``username`` when absent so the object is safe
    to build from the slim dict returned by ``authenticate_user`` as well as the
    full row from ``UserRepository.get_by_id``.
    """

    def __init__(self, user_dict: dict):
        self._data = user_dict
        self.id = str(user_dict["id"])
        self.username = user_dict["username"]
        self.full_name = user_dict.get("full_name") or user_dict["username"]
        self.is_admin = bool(user_dict.get("is_admin", False))

    # Flask-Login interface
    is_authenticated = True
    is_active = True
    is_anonymous = False

    def get_id(self):
        return self.id
