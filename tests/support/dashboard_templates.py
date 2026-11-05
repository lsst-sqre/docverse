"""Shared fixtures for dashboard template-set tests."""

from __future__ import annotations

__all__ = ["DEMO_SWITCHER_JINJA"]

DEMO_SWITCHER_JINJA = """\
[
  {"name": {{ editions.main.title | tojson }},
   "version": {{ editions.main.slug | tojson }},
   "url": {{ editions.main.published_url | tojson }},
   "preferred": true}
  {%- for release in editions.releases[:3] %},
  {"name": {{ (release.title ~ " of " ~ project.title ~ " (" ~ org.title ~ ")")
              | tojson }},
   "version": {{ release.slug | tojson }},
   "url": {{ release.published_url | tojson }}}
  {%- endfor %}
]
"""
"""A ``[switcher] template`` listing ``__main`` and the 3 newest releases.

``__main`` is the preferred entry, named by its title; each release is
named ``"<release title> of <project title> (<org title>)"`` so a render
proves the ``project`` and ``org`` variables reach the template. The
example in ``docs/dashboard-templates.md`` is the same shape.
"""
