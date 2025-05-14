from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Literal, Self, TypeAlias, overload, override

from ca_migrate import xml

Value: TypeAlias = str | int | datetime | bool


@dataclass(slots=True)
class Filter:
    """
    XOG Query Filter.


    Use the class methods to instantiate this class:

     - Filter.between
     - Filter.any
     - Filter.after
     - Filter.equals
    """

    column: str
    criteria: Literal["EQUALS", "OR", "BETWEEN", "AFTER"]
    value: Sequence[Value]

    custom_field: bool = False

    def to_xml(self) -> xml.Element:
        el = xml.create_element(
            "FilterByCustomInfo" if self.custom_field else "Filter",
            name=self.column,
            criteria=self.criteria,
        )
        serialize = partial(xml.serialize, custom_field=self.custom_field)
        el.text = ",".join(map(serialize, self.value))

        return el

    @overload
    @classmethod
    def between(
        cls, column: str, start: datetime, end: datetime, /, custom_field: bool = False
    ) -> Self: ...

    @overload
    @classmethod
    def between(
        cls, column: str, start: int, end: int, /, custom_field: bool = False
    ) -> Self: ...

    @overload
    @classmethod
    def between(
        cls, column: str, start: str, end: str, /, custom_field: bool = False
    ) -> Self: ...

    @classmethod
    def between(
        cls, column: str, start: Value, end: Value, /, custom_field: bool = False
    ) -> Self:
        """BETWEEN filter"""

        # Do not use spaces around comma-separated entries for OR and BETWEEN filters.
        return cls(
            column,
            "BETWEEN",
            [start, end],
            custom_field=custom_field,
        )

    @classmethod
    def any(
        cls, column: str, values: Sequence[Value], /, custom_field: bool = False
    ) -> Self:
        """OR filter"""
        return cls(
            column,
            "OR",
            values,
            custom_field=custom_field,
        )

    @classmethod
    def equals(cls, column: str, value: Value, /, custom_field: bool = False) -> Self:
        """EQUALS filter"""
        return cls(column, "EQUALS", [value], custom_field=custom_field)

    @classmethod
    def after(
        cls, column: str, value: str | int | datetime, /, custom_field: bool = False
    ) -> Self:
        """AFTER filter"""

        return cls(
            column,
            "AFTER",
            [value],
            custom_field=custom_field,
        )


@dataclass(slots=True)
class QueryType(Iterable[Filter]):
    queries: Sequence[Filter]

    def __len__(self) -> int:
        return len(self.queries)

    @override
    def __iter__(self) -> Iterator[Filter]:
        return iter(self.queries)


class Query(QueryType): ...


class LookupQuery(QueryType):
    @classmethod
    def any(cls, queries: Sequence[str]) -> Self:
        """

        Shortcut for:

        ```python
        query = LookupQuery([Filter.any("code", ["lookup1", "lookup2"])])
        ```
        """
        return cls([Filter.any("code", queries)])


@dataclass(slots=True)
class DataBus:
    @dataclass(slots=True)
    class Header:
        object_type: Literal["otherInvestment", "project", "contentPack"]
        args: dict[str, Value] = field(default_factory=dict)

        version: str = "8.0"
        action: Literal["read", "write"] = "read"

    header: Header
    query: Sequence[QueryType]

    def __post_init__(self):
        if len(self.query) == 0:
            raise ValueError("There should be at least one query filter to DataBus")

    def as_xml(self) -> xml.Element:
        databus = xml.create_element("NikuDataBus")
        # databus.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

        header = xml.create_element(
            "Header",
            version=self.header.version,
            action=self.header.action,
            objectType=self.header.object_type,
            externalSource="NIKU",
            parent=databus,
        )

        for name, value in self.header.args.items():
            _ = xml.create_element(
                "args", name=name, value=xml.serialize(value), parent=header
            )

        for query in self.query:
            query_el = xml.create_element(type(query).__name__, parent=databus)

            for filter in query:
                query_el.append(filter.to_xml())

        return databus


class ContentPack:
    __slots__: tuple[str] = ("queries",)

    def __init__(self, *queries: QueryType) -> None:
        self.queries: dict[type[QueryType], list[Filter]] = defaultdict(list)

        for query in queries:
            self.queries[type(query)].extend(query.queries)

    def to_databus(self) -> DataBus:
        if len(self.queries) == 0:
            raise ValueError("Expected at least one content pack query")

        return DataBus(
            header=DataBus.Header(
                object_type="contentPack",
            ),
            query=[t(values) for t, values in self.queries.items()],
        )

    def as_xml(self) -> xml.Element:
        return self.to_databus().as_xml()


equals = Filter.equals
between = Filter.between
any_match = Filter.any
after = Filter.after


def Project(
    *filters: Filter,
    order_by_1: str = "name",
    order_by_2: str = "projectID",
    include_tasks: bool = False,
    include_dependencies: bool = False,
    include_subprojects: bool = False,
    include_resources: bool = False,
    include_baselines: bool = False,
    include_allocations: bool = False,
    include_estimates: bool = False,
    include_actuals: bool = False,
    include_custom: bool = False,
    include_burdening: bool = False,
) -> DataBus:
    header = DataBus.Header(
        object_type="project",
        args={
            "order_by_1": order_by_1,
            "order_by_2": order_by_2,
            "include_tasks": include_tasks,
            "include_dependencies": include_dependencies,
            "include_subprojects": include_subprojects,
            "include_resources": include_resources,
            "include_baselines": include_baselines,
            "include_allocations": include_allocations,
            "include_estimates": include_estimates,
            "include_actuals": include_actuals,
            "include_custom": include_custom,
            "include_burdening": include_burdening,
        },
    )
    return DataBus(
        header=header,
        query=[Query(filters)],
    )


def Other(
    filters: Sequence[Filter],
    /,
    order_by_1: str = "name",
    order_by_2: str = "objectID",
    include_resources: bool = False,
    include_tasks: bool = False,
    include_allocations: bool = False,
) -> DataBus:
    header = DataBus.Header(
        object_type="project",
        args={
            "order_by_1": order_by_1,
            "order_by_2": order_by_2,
            "include_resources": include_resources,
            "include_tasks": include_tasks,
            "include_allocations": include_allocations,
        },
    )
    return DataBus(
        header=header,
        query=[Query(filters)],
    )


# <?xml version="1.0" encoding="UTF-8"?>
# <NikuDataBus xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="../xsd/nikuxog_read.xsd">
#   <Header version="6.0.11" action="read" objectType="project" externalSource="NIKU">
#     <!-- you change the order by simply swap 1 and 2 number in the name attribute -->
#     <args name="order_by_1" value="name"/>
#     <args name="order_by_2" value="projectID"/>
#     <args name="include_tasks" value="true"/>
#     <args name="include_dependencies" value="true"/>
#     <args name="include_subprojects" value="true"/>
#     <args name="include_resources" value="true"/>
#     <args name="include_baselines" value="true"/>
#     <args name="include_allocations" value="true"/>
#     <args name="include_estimates" value="true"/>
#     <args name="include_actuals" value="true"/>
#     <args name="include_custom" value="true"/>
#     <args name="include_burdening" value="false"/>
#   </Header>
#   <Query>
# <!--
#     <Filter name="active" criteria="EQUALS">true</Filter>
#     <FilterByCustomInfo name="project_billability" criteria="EQUALS">proj bill</FilterByCustomInfo>
#     <FilterByCustomInfo name="first_bill_date" criteria="BETWEEN">1999-01-07,2002-07-01</FilterByCustomInfo>
#     <FilterByCustomInfo name="project_risk" criteria="EQUALS">Medium</FilterByCustomInfo>
#     <FilterByCustomInfo name="profitable_project" criteria="EQUALS">true</FilterByCustomInfo>
#     <FilterByCustomInfo name="owner" criteria="EQUALS">last</FilterByCustomInfo>
#    -->
#
# <!--
#     <Filter name="active" criteria="EQUALS">true</Filter>
#     <Filter name="approved" criteria="EQUALS">true</Filter>
#     <Filter name="closed" criteria="EQUALS">false</Filter>
#     <Filter name="approvedForBilling" criteria="EQUALS">false</Filter>
#     <Filter name="projectID" criteria="EQUALS">test</Filter>
#     <Filter name="start" criteria="BETWEEN">1999-01-07,2001-01-15</Filter>
#     <Filter name="finish" criteria="EQUALS">Customer</Filter>
#     <Filter name="lastUpdatedDate" criteria="EQUALS">2002-01-26T11:42:03</Filter>
#     <Filter name="resourceID" criteria="EQUALS">jsmith</Filter>
#     <Filter name="trackMode" criteria="EQUALS">2</Filter>
#    -->
#   </Query>
# </NikuDataBus>

# <?xml version="1.0" encoding="UTF-8"?>
# <NikuDataBus xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="../xsd/nikuxog_read.xsd">
#   <Header version="7.0.1" action="read" objectType="otherInvestment" externalSource="NIKU">
#     <!-- you change the order by simply swap 1 and 2 number in the name attribute -->
#     <args name="order_by_1" value="name"/>
#     <args name="order_by_2" value="objectID"/>
#     <args name="include_resources" value="true"/>
#     <args name="include_tasks" value="true"/>
#     <args name="include_allocations" value="true"/>
#   </Header>
#   <Query>
#     <Filter name="objectID" criteria="EQUALS">Parkinglot</Filter>
#     <!--
# 	  <Filter name="objectID" criteria="EQUALS">test</Filter>
# 		<Filter name="status" criteria="EQUALS">1</Filter>
# 		<Filter name="lastUpdatedDate" criteria="EQUALS">2002-01-26T11:42:03</Filter>
#    -->
#   </Query>
# </NikuDataBus>
