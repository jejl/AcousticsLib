"""Base navbar factory shared across AstroAcoustics Dash applications.

Each app supplies its own brand and nav-item tree; this factory provides the
boilerplate (responsive toggler/collapse, user dropdown, sign-out) so the
structure and toggle-callback contract stay identical across apps.

The toggler/collapse use the fixed ids ``navbar-toggler`` / ``navbar-collapse``;
register the matching toggle callback once per app (typically in ``index.py``).
"""
import dash_bootstrap_components as dbc
from dash import html


def make_navbar(
    *,
    brand,
    brand_href: str,
    nav_items: list,
    user_label: str,
    user_menu_items: list,
    logout_href: str,
    color: str = "dark",
    dark: bool = True,
    sticky: str = "top",
    class_name: str = "mb-4",
):
    """Build a :class:`dbc.Navbar`.

    Args:
        brand:           Brand content (string or component list) for ``NavbarBrand``.
        brand_href:      Href for the brand link.
        nav_items:       List of nav components (``dbc.NavItem`` / ``dbc.DropdownMenu``)
                         placed before the user dropdown. App-specific menus
                         (incl. any admin menu) go here.
        user_label:      Label for the user dropdown (e.g. ``current_user.full_name``).
        user_menu_items: Items shown above the divider + Sign out (e.g. Settings,
                         Help, About).
        logout_href:     Href for the Sign out item.
    """
    user_dropdown = dbc.DropdownMenu(
        label=user_label,
        nav=True,
        in_navbar=True,
        children=[
            *user_menu_items,
            dbc.DropdownMenuItem(divider=True),
            dbc.DropdownMenuItem(
                [html.I(className="bi bi-box-arrow-right me-2"), "Sign out"],
                href=logout_href,
                # /logout is a plain Flask route, not a registered Dash page.
                # Without this, dbc's default client-side routing intercepts
                # the click and Dash's page_container renders its own 404
                # (no page matches "/logout") instead of hitting the real route.
                external_link=True,
            ),
        ],
    )

    return dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand(brand, href=brand_href, className="fw-semibold"),
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                dbc.Nav(
                    [*nav_items, user_dropdown],
                    navbar=True,
                    className="ms-auto",
                ),
                id="navbar-collapse",
                navbar=True,
                is_open=False,
            ),
        ], fluid=True),
        color=color,
        dark=dark,
        className=class_name,
        sticky=sticky,
    )
