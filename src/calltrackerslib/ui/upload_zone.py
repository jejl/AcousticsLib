"""Reusable native drop-zone component for Dash pages.

Pairs with assets/upload_zone.js (copy this file into each app's
dash_app/assets/ — Dash assets are app-local, there's no cross-app static
file serving) and the [data-dz-store].dz-active CSS rule. JS handles the
actual file reading and pushes accumulated file data into a dcc.Store via
dash_clientside.set_props.

Always use this instead of dcc.Upload — dcc.Upload silently drops all but
the first file on drag-and-drop (known Dash component bug).

Usage
-----
    from calltrackerslib.ui import make_upload_zone

    # In layout():
    make_upload_zone(
        zone_id   = "my-dropzone",
        store_id  = "my-files-store",
        accept    = ".csv,.txt",
        accept_label = ".csv or .txt",
    )

    # In callbacks: Input("my-files-store", "data") → list of {name, content}

    # Optional clear button (resets JS accumulator automatically):
    dbc.Button("Clear", id="my-clear-btn", **{"data-dz-reset": "my-dropzone"})
    # Wire callback: Output("my-files-store", "data") → []

The store is included in the returned component so it lives in the same
layout scope as the drop zone — put the whole component in a stable layout
(not inside a callback output) if the file list must survive panel
re-renders.
"""
from __future__ import annotations

from dash import dcc, html


_DEFAULT_STYLE: dict = {
    "width": "100%",
    "padding": "20px 0",
    "borderWidth": "2px",
    "borderStyle": "dashed",
    "borderRadius": "8px",
    "textAlign": "center",
    "borderColor": "#ccc",
    "cursor": "pointer",
    "backgroundColor": "#f7f6f2",
}


def make_upload_zone(
    zone_id: str,
    store_id: str,
    accept: str,
    accept_label: str | None = None,
    style: dict | None = None,
    className: str = "mb-2",
) -> html.Div:
    """Return a drop-zone div and its companion dcc.Store.

    Parameters
    ----------
    zone_id:      ID of the drop-zone div (must be unique on the page).
    store_id:     ID of the companion dcc.Store.
    accept:       File-type filter string for the hidden <input>, e.g. ".jpg,.png".
    accept_label: Human-readable label shown inside the drop zone.
                  Defaults to the raw ``accept`` string.
    style:        Override or extend the default drop-zone style dict.
    className:    Extra CSS classes on the outer wrapper (default "mb-2").
    """
    label = accept_label or accept
    merged_style = {**_DEFAULT_STYLE, **(style or {})}

    drop_zone = html.Div(
        [
            html.I(className="bi bi-cloud-upload me-2"),
            "Drag and drop files here, or click to select",
            html.Br(),
            html.Small(label, className="text-muted"),
        ],
        id=zone_id,
        style=merged_style,
        className=className,
        **{"data-dz-store": store_id, "data-dz-accept": accept},
    )

    store = dcc.Store(id=store_id, data=[])

    return html.Div([drop_zone, store])
