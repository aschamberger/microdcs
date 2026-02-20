# Auto-generated from "machinery_jobs.jsonschema.json". Do not modify!
from app.dataclass import DataClassConfig, DataClassMixin, DataClassResponseMixin
from app.models.machinery_jobs_mixin import JobStateMixin
from dataclasses import dataclass
from typing import Any
from enum import IntEnum


@dataclass(kw_only=True)
class AbortResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.10
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.AbortResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/AbortResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class LocalizedText(DataClassMixin):
    """
    https://reference.opcfoundation.org/v105/Core/docs/Part5/12.2.7
    """

    locale: str | None = None
    text: str | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "locale": "Locale",
            "text": "Text",
        }


@dataclass(kw_only=True)
class AbortCall(DataClassResponseMixin["AbortResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.10
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "AbortResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.AbortCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/AbortCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class EUInformation(DataClassMixin):
    """
    https://reference.opcfoundation.org/v105/Core/docs/Part8/5.6.3/#5.6.3.3
    """

    namespace_uri: str | None = None
    unit_id: int | None = None
    display_name: LocalizedText | None = None
    description: LocalizedText | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "namespace_uri": "NamespaceUri",
            "unit_id": "UnitId",
            "display_name": "DisplayName",
            "description": "Description",
        }


type OutputInfoType = int
"""
https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.4
"""


@dataclass(kw_only=True)
class OutputInformationDataType(DataClassMixin):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.3
    """

    item_number: str | None = None
    """
    ItemNumber defines an Identifier to identify the Type of the item (Material Identifier).
    """
    output_info: OutputInfoType | None = None
    """
    Bitmask indicating which of the optional fields are used for identification. If none is selected, only ItemNumber is used. Each selected optional field shall provide a value.
    """
    order_number: str | None = None
    """
    OrderNumber defines an Identifier to identify the order. Shall be provided if defined in OutputInfo.
    """
    lot_number: str | None = None
    """
    LotNumber defines an Identifier to identify the production-group of the item (Lot Identifier). Shall be provided if defined in OutputInfo.
    """
    serial_number: str | None = None
    """
    SerialNumber defines an Identifier to identify the one entity of the item (Product Identifier). Shall be provided if defined in OutputInfo.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "item_number": "ItemNumber",
            "output_info": "OutputInfo",
            "order_number": "OrderNumber",
            "lot_number": "LotNumber",
            "serial_number": "SerialNumber",
        }


@dataclass(kw_only=True)
class BOMComponentInformationDataType(DataClassMixin):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.5
    """

    identification: OutputInformationDataType | None = None
    """
    Identification of the output.
    """
    quantity: float | None = None
    """
    Quantity defines the amount of material. This quantity can be specified in different ways, e.g. weight or number.
    """
    engineering_units: EUInformation | None = None
    """
    The engineering unit of the quantity.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "identification": "Identification",
            "quantity": "Quantity",
            "engineering_units": "EngineeringUnits",
        }


@dataclass(kw_only=True)
class BOMInformationDataType(DataClassMixin):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.6
    """

    identification: OutputInformationDataType | None = None
    """
    Identification of the output.
    """
    component_information: list[BOMComponentInformationDataType] | None = None
    """
    Contains information about components.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "identification": "Identification",
            "component_information": "ComponentInformation",
        }


@dataclass(kw_only=True)
class CancelResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.12
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.CancelResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/CancelResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class CancelCall(DataClassResponseMixin["CancelResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.12
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "CancelResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.CancelCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/CancelCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class ClearResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.13
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.ClearResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/ClearResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class ClearCall(DataClassResponseMixin["ClearResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.13
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "ClearResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.ClearCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/ClearCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class ISA95PropertyDataType(DataClassMixin):
    """
    A subtype of OPC UA Structure that defines two linked data items: an ID, which is a unique identifier for a property within the scope of the associated resource, and the value, which is the data for the property.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.9
    """

    id: str | None = None
    """
    Unique identifier for a property within the scope of the associated resource
    """
    value: Any | None = None
    """
    Value for the property
    """
    description: list[LocalizedText] | None = None
    """
    An optional description of the parameter.
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the value
    """
    subproperties: list[ISA95PropertyDataType] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "value": "Value",
            "description": "Description",
            "engineering_units": "EngineeringUnits",
            "subproperties": "Subproperties",
        }


@dataclass(kw_only=True)
class ISA95EquipmentDataType(DataClassMixin):
    """
    Defines an equipment resource or a piece of equipment, a quantity, an optional description, and an optional collection of properties.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.1
    """

    id: str | None = None
    """
    An identification of an EquipmentClass or Equipment.
    """
    description: list[LocalizedText] | None = None
    """
    Additional information and description about the resource.
    """
    equipment_use: str | None = None
    """
    Information about the expected use of the equipment, see the ISA 95 Part 2 standard for defined values.
    """
    quantity: str | None = None
    """
    The quantity of the resource
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the quantity
    """
    properties: list[ISA95PropertyDataType] | None = None
    """
    Any associated properties, or empty if there are no properties defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "description": "Description",
            "equipment_use": "EquipmentUse",
            "quantity": "Quantity",
            "engineering_units": "EngineeringUnits",
            "properties": "Properties",
        }


@dataclass(kw_only=True)
class ISA95MaterialDataType(DataClassMixin):
    """
    Defines a material resource, a quantity, an optional description, and an optional collection of properties.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.6
    """

    material_class_id: str | None = None
    """
    An identification of the resource, or null if the Material Class is not used to identify the material.
    """
    material_definition_id: str | None = None
    """
    An identification of the resource, or null if the Material Definition is not used to identify the material.
    """
    material_lot_id: str | None = None
    """
    An identification of the resource, or null if the Material Lot is not used to identify the material.
    """
    material_sublot_id: str | None = None
    """
    An identification of the resource, or null if the Material Sublot is not used to identify the material.
    """
    description: list[LocalizedText] | None = None
    """
    Additional information and description about the resource.
    """
    material_use: str | None = None
    """
    Information about the expected use of the material, see the ISA 95 Part 2 standard for defined values.
    """
    quantity: str | None = None
    """
    The quantity of the resource
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the quantity
    """
    properties: list[ISA95PropertyDataType] | None = None
    """
    Any associated properties, or empty if there are no properties defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "material_class_id": "MaterialClassID",
            "material_definition_id": "MaterialDefinitionID",
            "material_lot_id": "MaterialLotID",
            "material_sublot_id": "MaterialSublotID",
            "description": "Description",
            "material_use": "MaterialUse",
            "quantity": "Quantity",
            "engineering_units": "EngineeringUnits",
            "properties": "Properties",
        }


@dataclass(kw_only=True)
class ISA95ParameterDataType(DataClassMixin):
    """
    A subtype of OPC UA Structure that defines three linked data items: the ID, which is a unique identifier for a property, the value, which is the data that is identified, and an optional description of the parameter.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.10
    """

    id: str | None = None
    """
    A unique identifier for a parameter
    """
    value: Any | None = None
    """
    Value of the parameter.
    """
    description: list[LocalizedText] | None = None
    """
    An optional description of the parameter. The array allows to provide descriptions in different languages when writing. When accessing, the server shall only provide one entry in the array.
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the value
    """
    subparameters: list[ISA95ParameterDataType] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "value": "Value",
            "description": "Description",
            "engineering_units": "EngineeringUnits",
            "subparameters": "Subparameters",
        }


@dataclass(kw_only=True)
class ISA95PersonnelDataType(DataClassMixin):
    """
    Defines a personnel resource or a person, a quantity, an optional description, and an optional collection of properties.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.7
    """

    id: str | None = None
    """
    An identification of a Personnel Class or Person.
    """
    description: list[LocalizedText] | None = None
    """
    Additional information and description about the resource.
    """
    personnel_use: str | None = None
    """
    Information about the expected use of the personnel, see the ISA 95 Part 2 standard for defined values.
    """
    quantity: str | None = None
    """
    The quantity of the resource
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the quantity
    """
    properties: list[ISA95PropertyDataType] | None = None
    """
    Any associated properties, or empty if there are no properties defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "description": "Description",
            "personnel_use": "PersonnelUse",
            "quantity": "Quantity",
            "engineering_units": "EngineeringUnits",
            "properties": "Properties",
        }


@dataclass(kw_only=True)
class ISA95PhysicalAssetDataType(DataClassMixin):
    """
    Defines a physical asset, a quantity, an optional description, and an optional collection of properties.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.8
    """

    id: str | None = None
    """
    An identification of a Physical Asset Class or Physical Asset.
    """
    description: list[LocalizedText] | None = None
    """
    Additional information and description about the resource.
    """
    physical_asset_use: str | None = None
    """
    Information about the expected use of the physical asset, see the ISA 95 Part 2 standard for defined values.
    """
    quantity: str | None = None
    """
    The quantity of the resource
    """
    engineering_units: EUInformation | None = None
    """
    The Unit Of Measure of the quantity
    """
    properties: list[ISA95PropertyDataType] | None = None
    """
    Any associated properties, or empty if there are no properties defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "description": "Description",
            "physical_asset_use": "PhysicalAssetUse",
            "quantity": "Quantity",
            "engineering_units": "EngineeringUnits",
            "properties": "Properties",
        }


@dataclass(kw_only=True)
class ISA95WorkMasterDataType(DataClassMixin):
    """
    Defines a Work Master ID and the defined parameters for the Work Master.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.11
    """

    id: str | None = None
    """
    An identification of the Work Master.
    """
    description: LocalizedText | None = None
    """
    Additional information and description about the Work Master.
    """
    parameters: list[ISA95ParameterDataType] | None = None
    """
    Defined parameters for the Work Master.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "id": "ID",
            "description": "Description",
            "parameters": "Parameters",
        }


@dataclass(kw_only=True)
class ISA95JobOrderDataType(JobStateMixin, DataClassMixin):
    """
    Defines the information needed to schedule and execute a job.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.4
    """

    job_order_id: str | None = None
    """
    An identification of the Job Order.
    """
    description: list[LocalizedText] | None = None
    """
    Addition information about the Job Order The array allows to provide descriptions in different languages.
    """
    work_master_id: list[ISA95WorkMasterDataType] | None = None
    """
    Work Master associated with the job order. If multiple work masters are defined, then the execution system can select the work master based on the availability of resources.
    """
    start_time: str | None = None
    """
    The proposed start time for the order, may be empty if not specified
    """
    end_time: str | None = None
    """
    The proposed end time for the order, may be empty if not specified
    """
    priority: int | None = None
    """
    The priority of the job order, may be empty of not specified. Higher numbers have higher priority.  This type allows the Job Order clients to pick their own ranges, and the Job Order server only has to pick the highest number.
    """
    job_order_parameters: list[ISA95ParameterDataType] | None = None
    """
    Key value pair with values, not associated with a resource that is provided as part of the job order, may be empty if not specified.
    """
    personnel_requirements: list[ISA95PersonnelDataType] | None = None
    """
    A specification of any personnel requirements associated with the job order, may be empty if not specified
    """
    equipment_requirements: list[ISA95EquipmentDataType] | None = None
    """
    A specification of any equipment requirements associated with the job order, may be empty if not specified.
    """
    physical_asset_requirements: list[ISA95PhysicalAssetDataType] | None = None
    """
    A specification of any physical asset requirements associated with the job order, may be empty if not specified.
    """
    material_requirements: list[ISA95MaterialDataType] | None = None
    """
    A specification of any material requirements associated with the job order, may be empty if not specified.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "description": "Description",
            "work_master_id": "WorkMasterID",
            "start_time": "StartTime",
            "end_time": "EndTime",
            "priority": "Priority",
            "job_order_parameters": "JobOrderParameters",
            "personnel_requirements": "PersonnelRequirements",
            "equipment_requirements": "EquipmentRequirements",
            "physical_asset_requirements": "PhysicalAssetRequirements",
            "material_requirements": "MaterialRequirements",
        }


@dataclass(kw_only=True)
class RelativePathElement(DataClassMixin):
    """
    https://reference.opcfoundation.org/v105/Core/docs/Part4/7.30
    """

    reference_type_id: str | None = None
    is_inverse: bool | None = None
    include_subtypes: bool | None = None
    target_name: str | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "reference_type_id": "ReferenceTypeId",
            "is_inverse": "IsInverse",
            "include_subtypes": "IncludeSubtypes",
            "target_name": "TargetName",
        }


@dataclass(kw_only=True)
class RelativePath(DataClassMixin):
    """
    https://reference.opcfoundation.org/v105/Core/docs/Part4/7.30
    """

    elements: list[RelativePathElement] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "elements": "Elements",
        }


@dataclass(kw_only=True)
class ISA95StateDataType(DataClassMixin):
    """
    Defines the information needed to schedule and execute a job.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.2
    """

    browse_path: RelativePath | None = None
    """
    The browse path of substates. Shall be null when the top-level state is represented.
    """
    state_text: LocalizedText | None = None
    """
    The state represented as human readable text. Shall represent the same text as the CurrentState Variable of a StateMachine would.
    """
    state_number: int | None = None
    """
    The state represented as number. Shall represent the same number as the Number subvariable of the CurrentState Variable of a StateMachine would.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "browse_path": "BrowsePath",
            "state_text": "StateText",
            "state_number": "StateNumber",
        }


@dataclass(kw_only=True)
class ISA95JobOrderAndStateDataType(DataClassMixin):
    """
    Defines the information needed to schedule and execute a job.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.3
    """

    job_order: ISA95JobOrderDataType | None = None
    """
    The job order
    """
    state: list[ISA95StateDataType] | None = None
    """
    The State of the job order. The array shall provide at least one entry representing the top level state and potentially additional entries representing substates. The first entry shall be the top level entry, having the BrowsePath set to Null. The order of the subtstates is not defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "job_order": "JobOrder",
            "state": "State",
        }


@dataclass(kw_only=True)
class ISA95JobResponseDataType(DataClassMixin):
    """
    Defines the information needed to schedule and execute a job.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.3.5
    """

    job_response_id: str | None = None
    """
    An identification of the Job Response
    """
    description: LocalizedText | None = None
    """
    Additional information about the Job Response
    """
    job_order_id: str | None = None
    """
    An identification of the job order associated with the job response.
    """
    start_time: str | None = None
    """
    The actual start time for the order.
    """
    end_time: str | None = None
    """
    The actual end time for the order.
    """
    job_state: list[ISA95StateDataType] | None = None
    """
    The current state of the job. The array shall provide at least one entry representing the top level state and potentially additional entries representing substates. The first entry shall be the top level entry, having the BrowsePath set to Null. The order of the subtstates is not defined.
    """
    job_response_data: list[ISA95ParameterDataType] | None = None
    """
    Key value pair with values, not associated with a resource that is provided as part of the job response, may be empty if not specified.
    """
    personnel_actuals: list[ISA95PersonnelDataType] | None = None
    """
    A specification of any personnel requirements associated with the job response, may be empty if not specified.
    """
    equipment_actuals: list[ISA95EquipmentDataType] | None = None
    """
    A specification of any equipment requirements associated with the job response, may be empty if not specified.
    """
    physical_asset_actuals: list[ISA95PhysicalAssetDataType] | None = None
    """
    A specification of any physical asset requirements associated with the job response, may be empty if not specified.
    """
    material_actuals: list[ISA95MaterialDataType] | None = None
    """
    A specification of any material requirements associated with the job response, may be empty if not specified.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "job_response_id": "JobResponseID",
            "description": "Description",
            "job_order_id": "JobOrderID",
            "start_time": "StartTime",
            "end_time": "EndTime",
            "job_state": "JobState",
            "job_response_data": "JobResponseData",
            "personnel_actuals": "PersonnelActuals",
            "equipment_actuals": "EquipmentActuals",
            "physical_asset_actuals": "PhysicalAssetActuals",
            "material_actuals": "MaterialActuals",
        }


@dataclass(kw_only=True)
class ISA95JobOrderStatusEventType(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.6
    """

    job_order: ISA95JobOrderDataType | None = None
    job_response: ISA95JobResponseDataType | None = None
    job_state: list[ISA95StateDataType] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = (
            "org.opcfoundation.ISA95-JOBCONTROL_V2.ISA95JobOrderStatusEventType.v2"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/ISA95JobOrderStatusEventType/"
        opcua_type: str = "Event"
        aliases: dict[str, str] = {
            "job_order": "JobOrder",
            "job_response": "JobResponse",
            "job_state": "JobState",
        }


class JobExecutionMode(IntEnum):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.1
    """

    SIMULATION_MODE = 0
    TEST_MODE = 1
    PRODUCTION_MODE = 2


@dataclass(kw_only=True)
class JobOrderControl(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.2/#7.2.2.2
    """

    current_state: LocalizedText | None = None
    last_transition: LocalizedText | None = None
    available_states: list[str] | None = None
    available_transitions: list[str] | None = None
    equipment_id: list[str] | None = None
    """
    Defines a read-only set of Equipment Class IDs and Equipment IDs that may be specified in a job order.
    """
    job_order_list: list[ISA95JobOrderAndStateDataType] | None = None
    """
    Defines a read-only list of job order information available from the server.
    """
    material_class_id: list[str] | None = None
    """
    Defines a read-only set of Material Classes IDs that may be specified in a job order.
    """
    material_definition_id: list[str] | None = None
    """
    Defines a read-only set of Material Classes IDs that may be specified in a job order.
    """
    max_downloadable_job_orders: int | None = None
    personnel_id: list[str] | None = None
    """
    Defines a read-only set of Personnel IDs and Person IDs that may be specified in a job order.
    """
    physical_asset_id: list[str] | None = None
    """
    Defines a read-only set of Physical Asset Class IDs and Physical Asset IDs that may be specified in a job order.
    """
    work_master: list[ISA95WorkMasterDataType] | None = None
    """
    Defines a read-only set of work master IDs that may be specified in a job order, and the read-only set of parameters that may be specified for a specific work master.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.Machinery.Jobs.JobOrderControl.v1"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/JobOrderControl/"
        opcua_type: str = "DataSet"
        opcua_state_machine: str = "{'states': {'Aborted': {'description': 'The job order is aborted.', 'number': 6}, 'AllowedToStart': {'description': 'The job order is stored and may be executed.', 'subStateMachine': {'description': 'Substates of AllowedToStart', 'states': {'Loaded': {'description': 'In situations where only one job may be in active memory and is able to be run, then the job is loaded in active memory, the necessary pre-conditions have been met, and the job order is ready to run, awaiting a Start command.', 'number': 3}, 'Ready': {'description': 'The necessary pre-conditions have been met and the job order is ready to run, awaiting a Start command.', 'number': 2}, 'Waiting': {'description': 'The necessary pre-conditions have not been met and the job order is not ready to run.', 'number': 1}}, 'transitions': {'FromLoadedToReady': {'description': 'This transition is triggered when the program or configuration to execute the job order is unloaded.', 'fromState': 'Loaded', 'toState': 'Ready', 'number': 4}, 'FromLoadedToWaiting': {'description': 'This transition is triggered when the system is not ready to start the execution of the job order anymore.', 'fromState': 'Loaded', 'number': 5, 'toState': 'Waiting'}, 'FromReadyToLoaded': {'description': 'This transition is triggered when the program or configuration to execute the job order is loaded.', 'toState': 'Loaded', 'fromState': 'Ready', 'number': 2}, 'FromReadyToWaiting': {'description': 'This transition is triggered when the system is not ready to start the execution of the job order anymore.', 'fromState': 'Ready', 'number': 3, 'toState': 'Waiting'}, 'FromWaitingToReady': {'description': 'This transition is triggered when the system is ready to start the execution of the job order.', 'toState': 'Ready', 'number': 1, 'fromState': 'Waiting'}}}, 'number': 2}, 'Ended': {'description': 'The job order has been completed and is no longer in execution.', 'subStateMachine': {'description': 'Substates of Ended', 'states': {'Closed': {'description': 'The job order has been completed and no further post processing is performed.', 'number': 2}, 'Completed': {'description': 'The job order has been completed and is no longer in execution.', 'number': 1}}, 'transitions': {'FromCompletedToClosed': {'description': 'This transition is triggered when the system has finalized post processing of a ended job order.', 'toState': 'Closed', 'fromState': 'Completed', 'number': 1}}}, 'number': 5}, 'Interrupted': {'description': 'The job order has been temporarily stopped.', 'subStateMachine': {'description': 'Substates of Interrupted', 'states': {'Held': {'description': 'The job order has been temporarily stopped due to a constraint of some form.', 'number': 1}, 'Suspended': {'description': 'The job order has been temporarily stopped due to a deliberate decision within the execution system.', 'number': 2}}, 'transitions': {'FromHeldToSuspended': {'description': 'This transition is triggered when the system has switched the job order from internally held to externally suspended, for example by a call of the Pause Method.', 'fromState': 'Held', 'toState': 'Suspended', 'number': 1}, 'FromSuspendedToHeld': {'description': 'This transition is triggered when the system has switched the job order from externally suspended to an internal held, for example by a call of the Resume Method.', 'toState': 'Held', 'fromState': 'Suspended', 'number': 2}}}, 'number': 4}, 'NotAllowedToStart': {'description': 'The job order is stored but may not be executed.', 'subStateMachine': {'description': 'Substates of NotAllowedToStart', 'states': {'Loaded': {'description': 'In situations where only one job may be in active memory and is able to be run, then the job is loaded in active memory, the necessary pre-conditions have been met, and the job order is ready to run, awaiting a Start command.', 'number': 3}, 'Ready': {'description': 'The necessary pre-conditions have been met and the job order is ready to run, awaiting a Start command.', 'number': 2}, 'Waiting': {'description': 'The necessary pre-conditions have not been met and the job order is not ready to run.', 'number': 1}}, 'transitions': {'FromLoadedToReady': {'description': 'This transition is triggered when the program or configuration to execute the job order is unloaded.', 'fromState': 'Loaded', 'toState': 'Ready', 'number': 4}, 'FromLoadedToWaiting': {'description': 'This transition is triggered when the system is not ready to start the execution of the job order anymore.', 'fromState': 'Loaded', 'number': 5, 'toState': 'Waiting'}, 'FromReadyToLoaded': {'description': 'This transition is triggered when the program or configuration to execute the job order is loaded.', 'toState': 'Loaded', 'fromState': 'Ready', 'number': 2}, 'FromReadyToWaiting': {'description': 'This transition is triggered when the system is not ready to start the execution of the job order anymore.', 'fromState': 'Ready', 'number': 3, 'toState': 'Waiting'}, 'FromWaitingToReady': {'description': 'This transition is triggered when the system is ready to start the execution of the job order.', 'toState': 'Ready', 'number': 1, 'fromState': 'Waiting'}}}, 'number': 1}, 'Running': {'description': 'The job order is executing.', 'number': 3}}, 'transitions': {'FromAllowedToStartToAborted': {'description': 'This transition is triggered when Abort Method is called.', 'cause': 'Abort', 'toState': 'Aborted', 'fromState': 'AllowedToStart', 'effect': 'ISA95JobOrderStatusEventType', 'number': 13}, 'FromAllowedToStartToAllowedToStart': {'description': 'This transition is triggered when the Update Method is called and the job order is modified.', 'fromState': 'AllowedToStart', 'toState': 'AllowedToStart', 'effect': 'ISA95JobOrderStatusEventType', 'number': 4, 'cause': 'Update'}, 'FromAllowedToStartToNotAllowedToStart': {'description': 'This transition is triggered when the RevokeStart Method is called.', 'fromState': 'AllowedToStart', 'effect': 'ISA95JobOrderStatusEventType', 'toState': 'NotAllowedToStart', 'cause': 'RevokeStart', 'number': 3}, 'FromAllowedToStartToRunning': {'description': 'This transition is triggered when a job order is started to be executed.', 'fromState': 'AllowedToStart', 'effect': 'ISA95JobOrderStatusEventType', 'toState': 'Running', 'number': 5}, 'FromInterruptedToAborted': {'description': 'This transition is triggered when Abort Method is called.', 'cause': 'Abort', 'toState': 'Aborted', 'fromState': 'Interrupted', 'effect': 'ISA95JobOrderStatusEventType', 'number': 9}, 'FromInterruptedToEnded': {'description': 'This transition is triggered when Stop Method is called.', 'toState': 'Ended', 'fromState': 'Interrupted', 'effect': 'ISA95JobOrderStatusEventType', 'cause': 'Stop', 'number': 11}, 'FromInterruptedToRunning': {'description': 'This transition is triggered when Resume Method is called.', 'fromState': 'Interrupted', 'effect': 'ISA95JobOrderStatusEventType', 'cause': 'Resume', 'toState': 'Running', 'number': 10}, 'FromNotAllowedToStartToAborted': {'description': 'This transition is triggered when Abort Method is called.', 'cause': 'Abort', 'toState': 'Aborted', 'effect': 'ISA95JobOrderStatusEventType', 'fromState': 'NotAllowedToStart', 'number': 12}, 'FromNotAllowedToStartToAllowedToStart': {'description': 'This transition is triggered when the Start Method is called.', 'toState': 'AllowedToStart', 'effect': 'ISA95JobOrderStatusEventType', 'fromState': 'NotAllowedToStart', 'cause': 'Start', 'number': 2}, 'FromNotAllowedToStartToNotAllowedToStart': {'description': 'This transition is triggered when the Update Method is called and the job order is modified.', 'effect': 'ISA95JobOrderStatusEventType', 'fromState': 'NotAllowedToStart', 'toState': 'NotAllowedToStart', 'number': 1, 'cause': 'Update'}, 'FromRunningToAborted': {'description': 'This transition is triggered when Abort Method is called.', 'cause': 'Abort', 'toState': 'Aborted', 'effect': 'ISA95JobOrderStatusEventType', 'fromState': 'Running', 'number': 8}, 'FromRunningToEnded': {'description': 'This transition is triggered when the execution of a job order has finished, either internally or by the Stop Method.', 'toState': 'Ended', 'effect': 'ISA95JobOrderStatusEventType', 'fromState': 'Running', 'cause': 'Stop', 'number': 7}, 'FromRunningToInterrupted': {'description': 'This transition is triggered when an executing job order gets interrupted, either internally or by the Pause Method.', 'toState': 'Interrupted', 'effect': 'ISA95JobOrderStatusEventType', 'cause': 'Pause', 'fromState': 'Running', 'number': 6}}}"
        opcua_state_machine_states: list[dict[str, Any]] = [
            # The job order is aborted.
            {"name": "Aborted"},
            # The job order is stored and may be executed.
            {
                "name": "AllowedToStart",
                "children": [
                    # In situations where only one job may be in active memory and is able to be run, then the job is loaded in active memory, the necessary pre-conditions have been met, and the job order is ready to run, awaiting a Start command.
                    {"name": "Loaded"},
                    # The necessary pre-conditions have been met and the job order is ready to run, awaiting a Start command.
                    {"name": "Ready"},
                    # The necessary pre-conditions have not been met and the job order is not ready to run.
                    {"name": "Waiting"},
                ],
                "transitions": [
                    # This transition is triggered when the program or configuration to execute the job order is unloaded.
                    {
                        "trigger": "AllowedToStartFromLoadedToReady",
                        "source": "Loaded",
                        "dest": "Ready",
                    },
                    # This transition is triggered when the system is not ready to start the execution of the job order anymore.
                    {
                        "trigger": "AllowedToStartFromLoadedToWaiting",
                        "source": "Loaded",
                        "dest": "Waiting",
                    },
                    # This transition is triggered when the program or configuration to execute the job order is loaded.
                    {
                        "trigger": "AllowedToStartFromReadyToLoaded",
                        "source": "Ready",
                        "dest": "Loaded",
                    },
                    # This transition is triggered when the system is not ready to start the execution of the job order anymore.
                    {
                        "trigger": "AllowedToStartFromReadyToWaiting",
                        "source": "Ready",
                        "dest": "Waiting",
                    },
                    # This transition is triggered when the system is ready to start the execution of the job order.
                    {
                        "trigger": "AllowedToStartFromWaitingToReady",
                        "source": "Waiting",
                        "dest": "Ready",
                    },
                ],
            },
            # The job order has been completed and is no longer in execution.
            {
                "name": "Ended",
                "children": [
                    # The job order has been completed and no further post processing is performed.
                    {"name": "Closed"},
                    # The job order has been completed and is no longer in execution.
                    {"name": "Completed"},
                ],
                "transitions": [
                    # This transition is triggered when the system has finalized post processing of a ended job order.
                    {
                        "trigger": "EndedFromCompletedToClosed",
                        "source": "Completed",
                        "dest": "Closed",
                    },
                ],
            },
            # The job order has been temporarily stopped.
            {
                "name": "Interrupted",
                "children": [
                    # The job order has been temporarily stopped due to a constraint of some form.
                    {"name": "Held"},
                    # The job order has been temporarily stopped due to a deliberate decision within the execution system.
                    {"name": "Suspended"},
                ],
                "transitions": [
                    # This transition is triggered when the system has switched the job order from internally held to externally suspended, for example by a call of the Pause Method.
                    {
                        "trigger": "InterruptedFromHeldToSuspended",
                        "source": "Held",
                        "dest": "Suspended",
                    },
                    # This transition is triggered when the system has switched the job order from externally suspended to an internal held, for example by a call of the Resume Method.
                    {
                        "trigger": "InterruptedFromSuspendedToHeld",
                        "source": "Suspended",
                        "dest": "Held",
                    },
                ],
            },
            # The job order is stored but may not be executed.
            {
                "name": "NotAllowedToStart",
                "children": [
                    # In situations where only one job may be in active memory and is able to be run, then the job is loaded in active memory, the necessary pre-conditions have been met, and the job order is ready to run, awaiting a Start command.
                    {"name": "Loaded"},
                    # The necessary pre-conditions have been met and the job order is ready to run, awaiting a Start command.
                    {"name": "Ready"},
                    # The necessary pre-conditions have not been met and the job order is not ready to run.
                    {"name": "Waiting"},
                ],
                "transitions": [
                    # This transition is triggered when the program or configuration to execute the job order is unloaded.
                    {
                        "trigger": "NotAllowedToStartFromLoadedToReady",
                        "source": "Loaded",
                        "dest": "Ready",
                    },
                    # This transition is triggered when the system is not ready to start the execution of the job order anymore.
                    {
                        "trigger": "NotAllowedToStartFromLoadedToWaiting",
                        "source": "Loaded",
                        "dest": "Waiting",
                    },
                    # This transition is triggered when the program or configuration to execute the job order is loaded.
                    {
                        "trigger": "NotAllowedToStartFromReadyToLoaded",
                        "source": "Ready",
                        "dest": "Loaded",
                    },
                    # This transition is triggered when the system is not ready to start the execution of the job order anymore.
                    {
                        "trigger": "NotAllowedToStartFromReadyToWaiting",
                        "source": "Ready",
                        "dest": "Waiting",
                    },
                    # This transition is triggered when the system is ready to start the execution of the job order.
                    {
                        "trigger": "NotAllowedToStartFromWaitingToReady",
                        "source": "Waiting",
                        "dest": "Ready",
                    },
                ],
            },
            # The job order is executing.
            {"name": "Running"},
        ]
        opcua_state_machine_state_ids: dict[str, str] = {
            "Aborted": "6",
            "AllowedToStart": "2",
            "AllowedToStart_Loaded": "2_3",
            "AllowedToStart_Ready": "2_2",
            "AllowedToStart_Waiting": "2_1",
            "Ended": "5",
            "Ended_Closed": "5_2",
            "Ended_Completed": "5_1",
            "Interrupted": "4",
            "Interrupted_Held": "4_1",
            "Interrupted_Suspended": "4_2",
            "NotAllowedToStart": "1",
            "NotAllowedToStart_Loaded": "1_3",
            "NotAllowedToStart_Ready": "1_2",
            "NotAllowedToStart_Waiting": "1_1",
            "Running": "3",
        }
        opcua_state_machine_transitions: list[dict[str, str]] = [
            # This transition is triggered when Abort Method is called.
            {"trigger": "Abort", "source": "AllowedToStart", "dest": "Aborted"},
            # This transition is triggered when the Update Method is called and the job order is modified.
            {"trigger": "Update", "source": "AllowedToStart", "dest": "AllowedToStart"},
            # This transition is triggered when the RevokeStart Method is called.
            {
                "trigger": "RevokeStart",
                "source": "AllowedToStart",
                "dest": "NotAllowedToStart",
            },
            # This transition is triggered when a job order is started to be executed.
            {
                "trigger": "FromAllowedToStartToRunning",
                "source": "AllowedToStart",
                "dest": "Running",
            },
            # This transition is triggered when Abort Method is called.
            {"trigger": "Abort", "source": "Interrupted", "dest": "Aborted"},
            # This transition is triggered when Stop Method is called.
            {"trigger": "Stop", "source": "Interrupted", "dest": "Ended"},
            # This transition is triggered when Resume Method is called.
            {"trigger": "Resume", "source": "Interrupted", "dest": "Running"},
            # This transition is triggered when Abort Method is called.
            {"trigger": "Abort", "source": "NotAllowedToStart", "dest": "Aborted"},
            # This transition is triggered when the Start Method is called.
            {
                "trigger": "Start",
                "source": "NotAllowedToStart",
                "dest": "AllowedToStart",
            },
            # This transition is triggered when the Update Method is called and the job order is modified.
            {
                "trigger": "Update",
                "source": "NotAllowedToStart",
                "dest": "NotAllowedToStart",
            },
            # This transition is triggered when Abort Method is called.
            {"trigger": "Abort", "source": "Running", "dest": "Aborted"},
            # This transition is triggered when the execution of a job order has finished, either internally or by the Stop Method.
            {"trigger": "Stop", "source": "Running", "dest": "Ended"},
            # This transition is triggered when an executing job order gets interrupted, either internally or by the Pause Method.
            {"trigger": "Pause", "source": "Running", "dest": "Interrupted"},
        ]
        opcua_state_machine_effects: dict[str, str] = {
            "Abort": "ISA95JobOrderStatusEventType",
            "Update": "ISA95JobOrderStatusEventType",
            "RevokeStart": "ISA95JobOrderStatusEventType",
            "FromAllowedToStartToRunning": "ISA95JobOrderStatusEventType",
            "Stop": "ISA95JobOrderStatusEventType",
            "Resume": "ISA95JobOrderStatusEventType",
            "Start": "ISA95JobOrderStatusEventType",
            "Pause": "ISA95JobOrderStatusEventType",
        }
        aliases: dict[str, str] = {
            "current_state": "CurrentState",
            "last_transition": "LastTransition",
            "available_states": "AvailableStates",
            "available_transitions": "AvailableTransitions",
            "equipment_id": "EquipmentID",
            "job_order_list": "JobOrderList",
            "material_class_id": "MaterialClassID",
            "material_definition_id": "MaterialDefinitionID",
            "max_downloadable_job_orders": "MaxDownloadableJobOrders",
            "personnel_id": "PersonnelID",
            "physical_asset_id": "PhysicalAssetID",
            "work_master": "WorkMaster",
        }


@dataclass(kw_only=True)
class JobOrderResults(DataClassMixin):
    """
    The OPENSCSJobResponseProviderObjectType contains a method to receive unsolicited job response requests.
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.7/#7.2.7.1
    """

    job_order_response_list: list[ISA95JobResponseDataType] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.Machinery.Jobs.JobOrderResults.v1"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/JobOrderResults/"
        opcua_type: str = "DataSet"
        aliases: dict[str, str] = {
            "job_order_response_list": "JobOrderResponseList",
        }


class JobResult(IntEnum):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.2
    """

    UNKNOWN = 0
    SUCCESSFUL = 1
    UNSUCCESSFUL = 2


class OutputInfoTypeMasks(IntEnum):
    ORDER_NUMBER = 1
    LOT_NUMBER = 2
    SERIAL_NUMBER = 4


@dataclass(kw_only=True)
class OutputPerformanceInfoDataType(DataClassMixin):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.7
    """

    identification: OutputInformationDataType | None = None
    """
    Identification of the output.
    """
    start_time: str | None = None
    """
    Output of first item from order.
    """
    end_time: str | None = None
    """
    Output of last item from order.
    """
    parameters: list[ISA95ParameterDataType] | None = None
    """
    Parameters specific to the performance like pressure or temperature.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "identification": "Identification",
            "start_time": "StartTime",
            "end_time": "EndTime",
            "parameters": "Parameters",
        }


@dataclass(kw_only=True)
class PauseResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.7
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.PauseResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/PauseResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class PauseCall(DataClassResponseMixin["PauseResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.7
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "PauseResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.PauseCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/PauseCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


class ProcessIrregularity(IntEnum):
    """
    https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/9.8
    """

    CAPABILITY_UNAVAILABLE = 0
    DETECTED = 1
    NOT_DETECTED = 2
    NOT_YET_DETERMINED = 3


@dataclass(kw_only=True)
class RequestJobResponseByJobOrderIDResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.7/#7.2.7.2
    """

    job_response: ISA95JobResponseDataType | None = None
    """
    Contains information about the execution of a job order, such as the current status of the job, actual material consumed, actual material produced, actual equipment used, and job specific data.
    """
    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = (
            "org.opcfoundation.Machinery.Jobs.RequestJobResponseByJobOrderIDResponse.v1"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/RequestJobResponseByJobOrderIDResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_response": "JobResponse",
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class RequestJobResponseByJobOrderIDCall(
    DataClassResponseMixin["RequestJobResponseByJobOrderIDResponse"], DataClassMixin
):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.7/#7.2.7.2
    """

    job_order_id: str | None = None
    """
    Contains an ID of the job order, as specified by the method caller.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "RequestJobResponseByJobOrderIDResponse"
        cloudevent_type: str = (
            "org.opcfoundation.Machinery.Jobs.RequestJobResponseByJobOrderIDCall.v1"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/RequestJobResponseByJobOrderIDCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
        }


@dataclass(kw_only=True)
class RequestJobResponseByJobOrderStateResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.7/#7.2.7.3
    """

    job_responses: list[ISA95JobResponseDataType] | None = None
    """
    Contains a list of information about the execution of a job order, such as the current status of the job, actual material consumed, actual material produced, actual equipment used, and job specific data.
    """
    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.Machinery.Jobs.RequestJobResponseByJobOrderStateResponse.v1"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/RequestJobResponseByJobOrderStateResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_responses": "JobResponses",
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class RequestJobResponseByJobOrderStateCall(
    DataClassResponseMixin["RequestJobResponseByJobOrderStateResponse"], DataClassMixin
):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.7/#7.2.7.3
    """

    job_order_state: list[ISA95StateDataType] | None = None
    """
    Contains a job status of the JobResponse to be returned. The array shall provide at least one entry representing the top level state and potentially additional entries representing substates. The first entry shall be the top level entry, having the BrowsePath set to null. The order of the substates is not defined.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "RequestJobResponseByJobOrderStateResponse"
        cloudevent_type: str = (
            "org.opcfoundation.Machinery.Jobs.RequestJobResponseByJobOrderStateCall.v1"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/Machinery/Jobs/v1.0.1/RequestJobResponseByJobOrderStateCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_state": "JobOrderState",
        }


@dataclass(kw_only=True)
class ResumeResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.8
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.ResumeResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/ResumeResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class ResumeCall(DataClassResponseMixin["ResumeResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.8
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "ResumeResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.ResumeCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/ResumeCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class RevokeStartResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.6
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = (
            "org.opcfoundation.ISA95-JOBCONTROL_V2.RevokeStartResponse.v2"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/RevokeStartResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class RevokeStartCall(DataClassResponseMixin["RevokeStartResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.6
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "RevokeStartResponse"
        cloudevent_type: str = (
            "org.opcfoundation.ISA95-JOBCONTROL_V2.RevokeStartCall.v2"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/RevokeStartCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class StartResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.5
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StartResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StartResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class StartCall(DataClassResponseMixin["StartResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.5
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "StartResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StartCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StartCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class StopResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.11
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StopResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StopResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class StopCall(DataClassResponseMixin["StopResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.11
    """

    job_order_id: str | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "StopResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StopCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StopCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order_id": "JobOrderID",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class StoreAndStartResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.4
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = (
            "org.opcfoundation.ISA95-JOBCONTROL_V2.StoreAndStartResponse.v2"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StoreAndStartResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class StoreAndStartCall(
    DataClassResponseMixin["StoreAndStartResponse"], DataClassMixin
):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.4
    """

    job_order: ISA95JobOrderDataType | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "StoreAndStartResponse"
        cloudevent_type: str = (
            "org.opcfoundation.ISA95-JOBCONTROL_V2.StoreAndStartCall.v2"
        )
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StoreAndStartCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order": "JobOrder",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class StoreResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.3
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StoreResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StoreResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class StoreCall(DataClassResponseMixin["StoreResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.3
    """

    job_order: ISA95JobOrderDataType | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "StoreResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.StoreCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/StoreCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order": "JobOrder",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class UpdateResponse(DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.9
    """

    return_status: int | None = None
    """
    Returns the status of the method execution.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.UpdateResponse.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/UpdateResponse/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "return_status": "ReturnStatus",
        }


@dataclass(kw_only=True)
class UpdateCall(DataClassResponseMixin["UpdateResponse"], DataClassMixin):
    """
    https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/7.2.1/#7.2.1.9
    """

    job_order: ISA95JobOrderDataType | None = None
    """
    Contains information defining the job order with all parameters and any material, equipment, or physical asset requirements associated with the order.
    """
    comment: list[LocalizedText] | None = None
    """
    The comment provides a description of why the method was called. In order to provide the comment in several languages, it is an array of LocalizedText. The array may be empty, when no comment is provided.
    """

    # mashumaro config class
    class Config(DataClassConfig):
        response_type: str = "UpdateResponse"
        cloudevent_type: str = "org.opcfoundation.ISA95-JOBCONTROL_V2.UpdateCall.v2"
        cloudevent_dataschema: str = "https://aschamberger.github.com/schemas/UA/ISA95-JOBCONTROL_V2/v2.0.0/UpdateCall/"
        opcua_type: str = "Method"
        aliases: dict[str, str] = {
            "job_order": "JobOrder",
            "comment": "Comment",
        }


@dataclass(kw_only=True)
class Meta(DataClassMixin):
    model_uri: str | None = "http://opcfoundation.org/UA/Machinery/Jobs/"
    model_version: str | None = "1.0.1"
    model_date: Any | None = "2024-05-01T00:00:00"
    additional_attributes: list[Any] | None = None

    # mashumaro config class
    class Config(DataClassConfig):
        aliases: dict[str, str] = {
            "model_uri": "modelUri",
            "model_version": "modelVersion",
            "model_date": "modelDate",
            "additional_attributes": "additionalAttributes",
        }


type Opc400013MachineryJobMgmtForMqtt = (
    Meta
    | JobOrderControl
    | AbortCall
    | AbortResponse
    | CancelCall
    | CancelResponse
    | ClearCall
    | ClearResponse
    | PauseCall
    | PauseResponse
    | ResumeCall
    | ResumeResponse
    | RevokeStartCall
    | RevokeStartResponse
    | StartCall
    | StartResponse
    | StopCall
    | StopResponse
    | StoreCall
    | StoreResponse
    | StoreAndStartCall
    | StoreAndStartResponse
    | UpdateCall
    | UpdateResponse
    | JobOrderResults
    | RequestJobResponseByJobOrderIDCall
    | RequestJobResponseByJobOrderIDResponse
    | RequestJobResponseByJobOrderStateCall
    | RequestJobResponseByJobOrderStateResponse
    | ISA95JobOrderStatusEventType
)
"""
A JSON Schema to represent the OPC 40001-3: Machinery Job Mgmt information model for a MQTT environment.
"""
