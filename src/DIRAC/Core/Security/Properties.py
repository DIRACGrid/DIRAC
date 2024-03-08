""" Just listing the possible Properties
This module contains list of Properties that can be assigned to users and groups
"""
from __future__ import annotations

import operator
from enum import Enum
from typing import Callable, Union


class SecurityProperty(str, Enum):
    #: A host property. This property is used::
    #: * For a host to forward credentials in an RPC call
    TRUSTED_HOST = "TrustedHost"
    #: Normal user operations
    NORMAL_USER = "NormalUser"
    #: CS Administrator - possibility to edit the Configuration Service
    CS_ADMINISTRATOR = "CSAdministrator"
    #: Job sharing among members of a group
    JOB_SHARING = "JobSharing"
    #: DIRAC Service Administrator
    SERVICE_ADMINISTRATOR = "ServiceAdministrator"
    #: Job Administrator can manipulate everybody's jobs
    JOB_ADMINISTRATOR = "JobAdministrator"
    #: Job Monitor - can get job monitoring information
    JOB_MONITOR = "JobMonitor"
    #: Accounting Monitor - can see accounting data for all groups
    ACCOUNTING_MONITOR = "AccountingMonitor"
    #: Generic pilot
    GENERIC_PILOT = "GenericPilot"
    #: Site Manager
    SITE_MANAGER = "SiteManager"
    #: User, group, VO Registry management
    USER_MANAGER = "UserManager"
    #: Operator
    OPERATOR = "Operator"
    #: Allow getting full delegated proxies
    FULL_DELEGATION = "FullDelegation"
    #: Allow getting only limited proxies (ie. pilots)
    LIMITED_DELEGATION = "LimitedDelegation"
    #: Allow getting only limited proxies for one self
    PRIVATE_LIMITED_DELEGATION = "PrivateLimitedDelegation"
    #: Allow managing proxies
    PROXY_MANAGEMENT = "ProxyManagement"
    #: Allow managing all productions
    PRODUCTION_MANAGEMENT = "ProductionManagement"
    #: Allow managing all productions in the same group
    PRODUCTION_SHARING = "ProductionSharing"
    #: Allows user to manage productions they own only
    PRODUCTION_USER = "ProductionUser"
    #: Allow production request approval on behalf of PPG
    PPG_AUTHORITY = "PPGAuthority"
    #: Allow Bookkeeping Management
    BOOKKEEPING_MANAGEMENT = "BookkeepingManagement"
    #: Allow FC Management - FC root user
    FC_MANAGEMENT = "FileCatalogManagement"
    #: Allow staging files
    STAGE_ALLOWED = "StageAllowed"

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

    def __and__(self, value: SecurityProperty | UnevaluatedProperty) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) & value

    def __or__(self, value: SecurityProperty | UnevaluatedProperty) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) | value

    def __xor__(self, value: SecurityProperty | UnevaluatedProperty) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) ^ value

    def __invert__(self: SecurityProperty) -> UnevaluatedExpression:
        return ~UnevaluatedProperty(self)


class UnevaluatedProperty:
    def __init__(self, property: SecurityProperty):
        self.property = property

    def __str__(self) -> str:
        return str(self.property)

    def __repr__(self) -> str:
        return repr(self.property)

    def __call__(self, allowed_properties: list[SecurityProperty]) -> bool:
        return self.property in allowed_properties

    def __and__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__and__, self, value)

    def __or__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__or__, self, value)

    def __xor__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__xor__, self, value)

    def __invert__(self) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__invert__, self)


class UnevaluatedExpression(UnevaluatedProperty):
    def __init__(self, operator: Callable[..., bool], *args: UnevaluatedProperty):
        self.operator = operator
        self.args = args

    def __str__(self) -> str:
        if self.operator == operator.__invert__:
            return f"~{self.args[0]}"
        symbol = {
            operator.__and__: "&",
            operator.__or__: "|",
            operator.__xor__: "^",
        }[self.operator]
        return f"({self.args[0]} {symbol} {self.args[1]})"

    def __repr__(self) -> str:
        return f"{self.operator.__name__}({', '.join(map(repr, self.args))})"

    def __call__(self, properties: list[SecurityProperty]) -> bool:
        return self.operator(*(a(properties) for a in self.args))


# Backwards compatibility hack
TRUSTED_HOST = SecurityProperty.TRUSTED_HOST.value
NORMAL_USER = SecurityProperty.NORMAL_USER.value
CS_ADMINISTRATOR = SecurityProperty.CS_ADMINISTRATOR.value
JOB_SHARING = SecurityProperty.JOB_SHARING.value
SERVICE_ADMINISTRATOR = SecurityProperty.SERVICE_ADMINISTRATOR.value
JOB_ADMINISTRATOR = SecurityProperty.JOB_ADMINISTRATOR.value
JOB_MONITOR = SecurityProperty.JOB_MONITOR.value
ACCOUNTING_MONITOR = SecurityProperty.ACCOUNTING_MONITOR.value
GENERIC_PILOT = SecurityProperty.GENERIC_PILOT.value
SITE_MANAGER = SecurityProperty.SITE_MANAGER.value
USER_MANAGER = SecurityProperty.USER_MANAGER.value
OPERATOR = SecurityProperty.OPERATOR.value
FULL_DELEGATION = SecurityProperty.FULL_DELEGATION.value
LIMITED_DELEGATION = SecurityProperty.LIMITED_DELEGATION.value
PRIVATE_LIMITED_DELEGATION = SecurityProperty.PRIVATE_LIMITED_DELEGATION.value
PROXY_MANAGEMENT = SecurityProperty.PROXY_MANAGEMENT.value
PRODUCTION_MANAGEMENT = SecurityProperty.PRODUCTION_MANAGEMENT.value
PPG_AUTHORITY = SecurityProperty.PPG_AUTHORITY.value
BOOKKEEPING_MANAGEMENT = SecurityProperty.BOOKKEEPING_MANAGEMENT.value
FC_MANAGEMENT = SecurityProperty.FC_MANAGEMENT.value
STAGE_ALLOWED = SecurityProperty.STAGE_ALLOWED.value
