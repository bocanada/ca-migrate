from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal, TypeAlias, overload

from lxml import etree as et

NS = {
    "xog": "http://www.niku.com/xog",
    "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

Element: TypeAlias = et._Element  # pyright: ignore[reportPrivateUsage]


def create_element(
    tag: str,
    ns: str | None = None,
    attrib: dict[str, str] | None = None,
    parent: Element | None = None,
    **extra: str,
):
    tag = et.QName(NS[ns], tag).text if ns is not None else tag

    if parent is None:
        return et.Element(tag, attrib=attrib, nsmap=NS, **extra)
    return et.SubElement(parent, tag, attrib=attrib, nsmap=NS, **extra)


def serialize(o: object, custom_field: bool = False):
    match o:
        case bool() if custom_field:
            # HELP: https://techdocs.broadcom.com/us/en/ca-enterprise-software/business-management/clarity-project-and-portfolio-management-ppm-on-demand/15-6/reference/xml-open-gateway-xog-development/xog-schema-sample-xml-files-and-special-characters.html#:~:text=Custom%20Boolean%20field
            return "1" if o else "0"
        case bool():
            return "true" if o else "false"
        case int():
            return str(o)
        case datetime():
            # https://techdocs.broadcom.com/us/en/ca-enterprise-software/business-management/clarity-project-and-portfolio-management-ppm-on-demand/15-6/reference/xml-open-gateway-xog-development/xog-schema-sample-xml-files-and-special-characters.html#:~:text=P2%5D%5D%20%3E%3C/ColumnValue%3E-,Date%20and%20Time%20Format,-Use%20this%20standard
            # 2002-01-26T11:42:03
            return o.strftime("%Y-%m-%dT%H:%M:%S")
        case date():
            return o.strftime("%Y-%m-%d")
        case str():
            return o
        case _:
            raise TypeError(
                f"Unexpected object of type {type(o)!r}, expected str, int, bool or datetime"
            )


def make_content_package_query(
    node: et.ElementBase,
    query_type: Literal["LookupQuery"],
    code: str | Sequence[str],
    /,
    filter_by: str = "code",
) -> et.ElementBase:
    criteria = "EQUALS" if isinstance(code, str) else "OR"
    code = ",".join(code) if not isinstance(code, str) else code

    lookup_query = et.SubElement(node, query_type)
    filter_node = et.SubElement(
        lookup_query, "Filter", name=filter_by, criteria=criteria
    )
    filter_node.text = code

    return node


@overload
def make_envelope(body_content: Element, session_id: str, /) -> Element: ...
@overload
def make_envelope(
    body_content: Element,
    username: str,
    password: str,
    /,
) -> Element: ...
@overload
def make_envelope(
    body_content: Element,
    /,
) -> Element: ...


def make_envelope(body_content: Element, *args: str) -> Element:
    root = create_element("Envelope", ns="soapenv")

    header = create_element("Header", ns="soapenv", parent=root)

    if len(args) > 0:
        auth = create_element("Auth", ns="xog", parent=header)
        if len(args) == 1:
            sesh_el = create_element("SessionID", ns="xog", parent=auth)
            sesh_el.text = args[0]
        elif len(args) == 2:
            for tag, value in zip(["Username", "Password"], args):
                el = create_element(tag, ns="xog", parent=auth)
                el.text = value
        else:
            pass

    body = create_element("Body", ns="soapenv", parent=root)
    body.append(body_content)

    return root


def to_bytes(el: Element, /, indent: bool = False) -> bytes:
    return et.tostring(
        el,
        encoding="UTF-8",
        method="xml",
        xml_declaration=True,
        pretty_print=indent,
    )
