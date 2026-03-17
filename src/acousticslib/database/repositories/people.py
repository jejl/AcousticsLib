"""Repositories for People, Owner, and Custodian tables."""
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from ...error_handlers import handle_repository_errors
from ..connection import get_session


class PeopleRepository:
    """Data access layer for calltrackers.People (observers)."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return id, PersName, UserID, Email, Phone for all people ordered by name."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, PersName, UserID, Email, Phone "
                    "FROM calltrackers.People ORDER BY PersName"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def create(
        pers_name: str,
        user_id: Optional[str],
        email: str,
        phone: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Insert a new person and return the refreshed full people list."""
        with get_session() as session:
            session.execute(
                text(
                    "INSERT INTO calltrackers.People (PersName, UserID, Email, Phone) "
                    "VALUES (:name, :uid, :email, :phone)"
                ),
                {"name": pers_name, "uid": user_id, "email": email, "phone": phone},
            )
            return session.execute(
                text(
                    "SELECT id, PersName, UserID, Email, Phone "
                    "FROM calltrackers.People"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def update(
        email: str,
        pers_name: str,
        user_id: Optional[str],
        phone: Optional[str],
    ) -> None:
        """Update an existing person's details, identified by email."""
        with get_session() as session:
            session.execute(
                text(
                    "UPDATE calltrackers.People "
                    "SET PersName=:name, UserID=:uid, Phone=:phone "
                    "WHERE Email=:email"
                ),
                {"name": pers_name, "uid": user_id, "phone": phone, "email": email},
            )


class OwnerRepository:
    """Data access layer for calltrackers.Owner."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all owners with a computed full name."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, "
                    "CONCAT("
                    "  CASE WHEN First_Name    IS NOT NULL THEN First_Name ELSE '' END, "
                    "  CASE WHEN Last_Name     IS NOT NULL THEN CONCAT(' ', Last_Name) ELSE '' END, "
                    "  CASE WHEN Business_Name IS NOT NULL THEN CONCAT(', ', Business_Name) ELSE '' END"
                    ") AS name, "
                    "First_Name, Last_Name, Business_Name "
                    "FROM calltrackers.Owner "
                    "ORDER BY First_Name, Last_Name, Business_Name"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_id(owner_id: int) -> Optional[Dict[str, Any]]:
        """Return a single owner by id, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.Owner WHERE id = :id"),
                {"id": owner_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def create(
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        business_name: Optional[str] = None,
    ) -> int:
        """Insert a new owner and return their id."""
        if not first_name and not business_name:
            raise ValueError("Either first_name or business_name must be provided")
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.Owner (First_Name, Last_Name, Business_Name) "
                    "VALUES (:first, :last, :biz)"
                ),
                {"first": first_name, "last": last_name, "biz": business_name},
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def get_name_mappings() -> Tuple[Dict[int, str], Dict[str, int]]:
        """Return ({id: name}, {name: id}) bidirectional mappings."""
        owners = OwnerRepository.get_all()
        id_to_name = {o["id"]: o["name"] for o in owners}
        name_to_id = {o["name"]: o["id"] for o in owners}
        return id_to_name, name_to_id


class CustodianRepository:
    """Data access layer for calltrackers.Custodian."""

    @staticmethod
    @handle_repository_errors
    def get_all() -> List[Dict[str, Any]]:
        """Return all custodians with a computed full name."""
        with get_session() as session:
            return session.execute(
                text(
                    "SELECT id, "
                    "CONCAT("
                    "  CASE WHEN FirstName IS NOT NULL THEN FirstName ELSE '' END, "
                    "  CASE WHEN LastName  IS NOT NULL THEN CONCAT(' ', LastName) ELSE '' END"
                    ") AS name, "
                    "FirstName, LastName "
                    "FROM calltrackers.Custodian "
                    "ORDER BY FirstName, LastName"
                )
            ).mappings().all()

    @staticmethod
    @handle_repository_errors
    def get_by_id(custodian_id: int) -> Optional[Dict[str, Any]]:
        """Return a single custodian by id, or None."""
        with get_session() as session:
            return session.execute(
                text("SELECT * FROM calltrackers.Custodian WHERE id = :id"),
                {"id": custodian_id},
            ).mappings().first()

    @staticmethod
    @handle_repository_errors
    def create(first_name: str, last_name: str) -> int:
        """Insert a new custodian and return their id."""
        if not first_name or not last_name:
            raise ValueError("Both first_name and last_name are required")
        with get_session() as session:
            result = session.execute(
                text(
                    "INSERT INTO calltrackers.Custodian (FirstName, LastName) "
                    "VALUES (:first, :last)"
                ),
                {"first": first_name, "last": last_name},
            )
            return result.lastrowid

    @staticmethod
    @handle_repository_errors
    def get_name_mappings() -> Tuple[Dict[int, str], Dict[str, int]]:
        """Return ({id: name}, {name: id}) bidirectional mappings."""
        custodians = CustodianRepository.get_all()
        id_to_name = {c["id"]: c["name"] for c in custodians}
        name_to_id = {c["name"]: c["id"] for c in custodians}
        return id_to_name, name_to_id
