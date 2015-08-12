============================
Regular Expression Functions
============================

All of the regular expression functions use the `Java pattern`_ syntax,
with a few notable exceptions:

* When using multi-line mode (enabled via the ``(?m)`` flag),
  only ``\n`` is recognized as a line terminator. Additionally,
  the ``(?d)`` flag is not supported and must not be used.
* Case-insensitive matching (enabled via the ``(?i)`` flag) is always
  performed in a Unicode-aware manner. However, context-sensitive and
  local-sensitive matching is not supported. Additionally, the
  ``(?u)`` flag is not supported and must not be used.
* Surrogate pairs are not supported. For example, ``\uD800\uDC00`` is
  not treated as ``U+10000`` and must be specified as ``\x{10000}``.
* Boundaries (``\b``) are incorrectly handled for a non-spacing mark
  without a base character.
* ``\Q`` and ``\E`` are not supported in character classes
  (such as ``[A-Z123]``) and are instead treated as literals.
* Unicode character classes (``\p{prop}``) are supported with
  the following differences:

  * All underscores in names must be removed. For example, use
    ``OldItalic`` instead of ``Old_Italic``.
  * Scripts must be specified directly, without the
    ``Is``, ``script=`` or ``sc=`` prefixes.
    Example: ``\p{Hiragana}``
  * Blocks must be specified with the ``In`` prefix.
    The ``block=`` and ``blk=`` prefixes are not supported.
    Example: ``\p{Mongolian}``
  * Categories must be specified directly, without the ``Is``,
    ``general_category=`` or ``gc=`` prefixes.
    Example: ``\p{L}``
  * Binary properties must be specified directly, without the ``Is``.
    Example: ``\p{NoncharacterCodePoint}``

    .. _Java pattern: http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html

    .. _capturing group number: http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#gnumber

.. function:: regexp_extract_all(string, pattern) -> array<varchar>

    Returns the substring(s) matched by the regular expression ``pattern``
    in ``string``.

.. function:: regexp_extract_all(string, pattern, group) -> array<varchar>

    Finds all occurrences of the regular expression ``pattern`` in ``string``
    and returns the `capturing group number`_ ``group``.

.. function:: regexp_extract(string, pattern) -> varchar

    Returns the first substring matched by the regular expression ``pattern``
    in ``string``.

.. function:: regexp_extract(string, pattern, group) -> varchar

    Finds the first occurrence of the regular expression ``pattern`` in
    ``string`` and returns the `capturing group number`_ ``group``.

.. function:: regexp_like(string, pattern) -> boolean

    Evaluates the regular expression ``pattern`` and determines if it is
    contained within ``string``.

    This function is similar to the ``LIKE`` operator, expect that the
    pattern only needs to be contained within ``string``, rather than
    needing to match all of ``string``. In other words, this performs a
    *contains* operation rather than a *match* operation. You can match
    the entire string by anchoring the pattern using ``^`` and ``$``.

.. function:: regexp_replace(string, pattern) -> varchar

    Removes every instance of the substring matched by the regular expression
    ``pattern`` from ``string``.

.. function:: regexp_replace(string, pattern, replacement) -> varchar

    Replaces every instance of the substring matched by the regular expression
    ``pattern`` in ``string`` with ``replacement``. `Capturing groups`_ can be
    referenced in ``replacement`` using ``$g`` for a numbered group or
    ``${name}`` for a named group. A dollar sign (``$``) may be included in the
    replacement by escaping it with a backslash (``\$``).

    .. _Capturing groups: http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#cg

.. function:: regexp_split(string, pattern) -> array<varchar>

    Splits ``string`` using the regular expression ``pattern`` and returns an
    array. Trailing empty strings are preserved.
