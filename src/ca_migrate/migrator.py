from collections.abc import Callable, Generator
from dataclasses import dataclass
from types import TracebackType
from typing import Self, override

from httpx import (
    URL,
    USE_CLIENT_DEFAULT,
    AsyncClient,
    Auth,
    Cookies,
    Request,
    Response,
    Timeout,
)
from lxml import etree as et

from ca_migrate import xml
from ca_migrate.objects import Xoggable
from ca_migrate.objects import xml as xog


class MigrationException(Exception): ...


@dataclass(slots=True)
class XogFailureException(MigrationException):
    """XogFailureException means we couldn't get any info as to why this XOG failed"""

    doc: xml.Element

    @override
    def __str__(self) -> str:
        return "Failed running XOG"


@dataclass(slots=True)
class XogException(MigrationException):
    severity: str
    description: str

    doc: xml.Element

    @override
    def __str__(self) -> str:
        return self.description


@dataclass(slots=True)
class AuthException(MigrationException):
    doc: xml.Element

    @override
    def __str__(self) -> str:
        return "Invalid username or password."


class SessionIDAuth(Auth):
    def __init__(self, token: str) -> None:
        self.session_id: str = token

    @override
    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        request.headers["Authtoken"] = self.session_id

        Cookies({"sessionId": self.session_id}).set_cookie_header(request)
        yield request


class Client:
    __slots__: tuple[str, ...] = ("base_url", "client", "xog_ctx", "api_ctx", "auth")

    def __init__(
        self,
        base_url: URL | str,
        xog_ctx: str = "/niku/xog",
        api_ctx: str = "/ppm/rest/",
        verify: bool = True,
        timeout: Timeout | float | None = None,
        session_id: str | None = None,
    ) -> None:
        self.xog_ctx: str = xog_ctx
        self.api_ctx: str = api_ctx
        self.base_url: str | URL = base_url
        self.client: AsyncClient = AsyncClient(
            base_url=self.base_url, verify=verify, timeout=timeout
        )

        self.auth: SessionIDAuth | None = (
            SessionIDAuth(session_id) if session_id is not None else None
        )

    async def login(self, username: str, password: str) -> Self:
        """
        Log in via XOG and use the Session ID to access both XOG and the REST API.
        """
        login_el = xml.create_element("Login", ns="xog")
        user_el = xml.create_element("Username", ns="xog", parent=login_el)
        pass_el = xml.create_element("Password", ns="xog", parent=login_el)

        user_el.text = username
        pass_el.text = password

        try:
            resp = await self.run_xog(login_el)
        except XogException as e:
            raise AuthException(e.doc) from e

        session_id = resp.findtext(".//xog:SessionID", namespaces=xml.NS)

        if session_id is None:
            raise AuthException(resp)

        self.auth = SessionIDAuth(session_id)

        return self

    async def logout(self) -> None:
        """
        Log in via XOG and use the Session ID to access both XOG and the REST API.
        """
        if self.auth is None:
            return

        logout_el = xml.create_element("Logout", ns="xog")
        _ = await self.run_xog(logout_el)

    async def run_xog(self, data: xml.Element) -> xml.Element:
        if self.auth is None:
            data = xml.make_envelope(data)
        else:
            data = xml.make_envelope(
                data,
                self.auth.session_id,
            )

        content = xml.to_bytes(data, indent=True)

        auth = USE_CLIENT_DEFAULT if self.auth is None else self.auth
        resp = await self.client.post(
            self.xog_ctx,
            auth=auth,
            content=content,
            headers={"Content-Type": "text/xml; charset=utf-8"},
        )
        resp = resp.raise_for_status()

        return try_error(et.fromstring(resp.content))

    async def __aenter__(self):
        self.client = await self.client.__aenter__()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ):
        _ = await self.logout()

        await self.client.__aexit__(exc_type, exc_value, traceback)

        return None


@dataclass
class Xogger:
    src: Client
    dest: Client

    async def migrate(
        self,
        content: Xoggable,
        transform_fn: Callable[[xml.Element], xml.Element] | None = None,
    ) -> xml.Element:
        """
        # Parameters
        - `content`: content to XOG from `src` to `dest`.
        - `clean_fn`: a function to transform the XOG response received from `src`.

        """
        # TODO: throw a useful exception instead
        assert self.src.auth is not None, "src is not logged in. call `self.src.login`"

        match content:
            case xog.ContentPack():
                databus = content.to_databus()
                return await self.migrate(databus)
            case xog.DataBus():
                # TODO: handle pagination:
                # https://techdocs.broadcom.com/us/en/ca-enterprise-software/business-management/clarity-project-and-portfolio-management-ppm-on-premise/16-3-2/reference/xml-open-gateway-xog-development/xog-governor-node-limit.html#:~:text=Copy-,XOG%20Read,-For%20a%20XOG
                content_pack = content.as_xml()
                resp = await self.src.run_xog(content_pack)
                if transform_fn is not None:
                    resp = transform_fn(resp)

                assert self.dest.auth is not None, (
                    "dest is not logged in. call `self.dest.login`"
                )
                return await self.dest.run_xog(resp)
            case _:
                raise NotImplementedError(content)


def get_databus(root: xml.Element) -> xml.Element:
    databus = root.find("./soapenv:Body/NikuDataBus", namespaces=xml.NS)
    if databus is not None:
        return databus
    return root


def try_error(el: xml.Element) -> xml.Element:
    xog_output = el.find(".//XOGOutput", namespaces=xml.NS)
    if xog_output is not None:
        status = xog_output.find("Status")
        if status is not None:
            status_state = status.get("state", "OK")

            if status_state == "FAILURE":
                error_info = xog_output.find("ErrorInformation")
                if error_info is None:
                    raise XogFailureException(el)

                raise XogException(
                    severity=error_info.findtext("Severity", "FATAL"),
                    description=error_info.findtext(
                        "Description", "Failed running XOG"
                    ),
                    doc=el,
                )

    return get_databus(el)
