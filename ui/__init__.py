"""Streamlit UI components for Squeeze the Line.

The pieces of the (formerly monolithic) ``app.py`` rendering layer live here,
split by concern: the picks board, player detail page, charts, filters, the
player comparison and what-if views, and the odds-only injuries view. ``app.py``
is the thin entry point that wires these together; the Streamlit-free data
pipeline lives in the top-level ``pipeline`` module.
"""
