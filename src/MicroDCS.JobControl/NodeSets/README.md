# Vendored NodeSets (D-34)

| File | Source | Pin |
|---|---|---|
| `Opc.Ua.ISA95.JobControl.V2.NodeSet2.xml` | `OPCFoundation/UA-.NETStandard`, `src/Opc.Ua.ISA95/Design/JobControl/V2/`, tag `2.0.1`, commit `07e62f72500cc0923a2bab940653c8c1116fa286` | Model `http://opcfoundation.org/UA/ISA95-JOBCONTROL_V2/`, version 2.0.0, published 2024-01-31, 258 nodes. OPC Foundation MIT License 1.00 |
| `MicroDcs.JobControl.NodeSet2.xml` | This repository | Model `urn:microdcs:jobcontrol`, requires Job Control V2 |

The V2 file is taken from the stack repository because the generated types come from it. `tests/MicroDCS.JobControl.Tests` pins it.
After replacing it, regenerate the table:

```bash
dotnet run --project tools/MicroDCS.JobControl.TableGen -- src/MicroDCS.JobControl/NodeSets/Opc.Ua.ISA95.JobControl.V2.NodeSet2.xml src/MicroDCS.JobControl/StateMachine/JobControlV2Transitions.g.cs
```
