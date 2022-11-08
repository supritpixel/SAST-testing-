using server.api.DAL.Xian.Models;
using Microsoft.EntityFrameworkCore;
using server.api.core.Models;
using server.api.core.interfaces;
using server.api.core.Interfaces;
using server.api.core.models;
using server.api.DAL.Xian.Common;
using server.api.DAL.Xian.EF;
using solvexia.svx.shared.lib.sharedutilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;
using svx.lib.dalconnector;
using svx.lib.logging;

namespace server.api.DAL.Xian
{
    public class StepsDAL : DALBase, IStepsDAL
    {
        #region Declarations

        private readonly IAuthorisation _authorisationDAL;
        private readonly ILogger<StepsDAL> _logger;

        #endregion Declarations

        #region LifeCycle
        public StepsDAL(IDALConnector dalConnector, IAuthorisation authorisationDAL, IUserRoleManager userRoleManager, ILogger<StepsDAL> logger) : base(dalConnector, userRoleManager)
        {
            _authorisationDAL = authorisationDAL;
            _logger = logger;
        }
        #endregion LifeCycle


        #region Interface Methods

        public async Task<Dictionary<Pointer, core.models.StepStatusInfo>> GetStepsStatus(Pointer user, IEnumerable<Pointer> stepXianPointers)
        {
            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepsRunTimeData = await GetStepRunTimeData(db, stepXianPointers);

                return stepsRunTimeData
                    .Select(x => new { stepPointer = x.Key, stepRunStatus = new core.models.StepStatusInfo(x.Value, x.Key) })
                    .ToDictionary(x => x.stepPointer, x => x.stepRunStatus);
            }
        }

        public async Task<StepRuntimeData> CancelRunStep(Pointer user, Pointer step)
        {
            await _authorisationDAL.Demand(user,
                step,
                new UserRoleEnum[] { UserRoleEnum.Subscriber, UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                UserPermissionRoleTypeEnum.Editor,
                UserPermissionRoleTypeEnum.Executor,
                UserPermissionRoleTypeEnum.Owner);

            var stepXianId = step.GetId<int>();

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXian = await GetStepXian(db, stepXianId, throwExceptionIfNotFound: true);
                var parentProcess = new Pointer(EntityType.Process, (int)stepXian.Owner);

                var executionIds = await GetRunningExecutions(db, stepXianId, parentProcess.GetId<int>());

                await CancelExecution(user.GetId<int>(), executionIds);

                return await GetStepRunTimeData(db, parentProcess.GetId<int>(), stepXianId);
            }
        }

        public async Task MoveStep(Pointer user, Pointer group, Pointer step, int stepOrder)
        {
            await _authorisationDAL.Demand(user, step,
                new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                                   UserPermissionRoleTypeEnum.Owner, UserPermissionRoleTypeEnum.Editor);

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                int stepId = step.GetId<int>();
                var stepXian = db.Xians.SingleOrDefault(a => a.XianID == step.GetId<int>() && a.DateDeleted == null);
                if (stepXian == null)
                {
                    throw new ApiException(ApiExceptionReason.InvalidParameters, "Step does not exist");
                }
                var process = new Pointer(EntityType.Process, (int)stepXian.Owner);

                var processSpecXianValue = await GetXianValues(db, process.GetId<int>(), new[] { "ProjectSpecificationChangeSummary", "ProjectSpecification" });

                var processSpecXml = XElement.Parse(processSpecXianValue["ProjectSpecification"]?.Value);

                int originalIndex = 1;
                XElement foundStepXML = null;
                bool foundStep = false;
                foreach (var elem in processSpecXml.Elements("Operation"))
                {
                    originalIndex = 1;
                    foreach (var elemStep in elem.Elements("Step"))
                    {
                        if (elemStep.Attribute("StepXianID").Value == stepId.ToString())
                        {
                            foundStepXML = elemStep;
                            foundStep = true;
                            break;
                        }
                       
                        originalIndex++;
                    }
                    if (foundStep == true)
                    {
                        break;
                    }
                }
                       
                var stepOriginalGroup = foundStepXML.Parent;
               
                var foundGroupXML = processSpecXml.Elements("Operation").Single(a => a.Attribute("GroupId").Value.ToLower() == group.GetId<string>().ToLower().ToString());
                int stepAmountInGroup=foundGroupXML.Elements("Step").Count();
                var copyOfStepXML = new XElement(foundStepXML);
                foundStepXML.Remove();
                var changeHistory = GetEmptyLastChangeSummary();
                changeHistory.Add(GetCustomChangeElement($"Step {step.GetId<int>()}",
    $"Step <b>{stepXian.Name}</b> was moved from position <b>{originalIndex}</b> in operation group <b>{stepOriginalGroup.Attribute("Name").Value}</b> to position <b>{stepOrder+1}</b> in operation group <b>{foundGroupXML.Attribute("Name").Value}</b>"));
                if (stepOrder == 0)
                {
                    foundGroupXML.AddFirst(copyOfStepXML);
                }
                else
                {
                    foundGroupXML.Elements("Step").ElementAt(stepOrder-1).AddAfterSelf(copyOfStepXML);
                    //if (stepAmountInGroup > stepOrder)
                    //{
                    //    foundGroupXML.Elements("Step").ElementAt(stepOrder).AddAfterSelf(copyOfStepXML);
                   
                    //}
                    //else
                    //{
                    //    foundGroupXML.Elements("Step").ElementAt(stepAmountInGroup-1).AddAfterSelf(copyOfStepXML);                                                                                        
                    //}
                }
                


                RefreshProcessGroupXMLStepOrder(db,stepOriginalGroup);
                //if we moved a step inside its own group, then we only reorder that group xml, otherwise we have to reorder both original and target group
                if (stepOriginalGroup.Attribute("GroupId").Value!= foundGroupXML.Attribute("GroupId").Value)
                {
                    RefreshProcessGroupXMLStepOrder(db, foundGroupXML);
                }
              //  processSpecXml = RefreshStepDependancyInXmlForPriorTypes(processSpecXml);
                processSpecXianValue["ProjectSpecification"].Value = processSpecXml.ToString();
                processSpecXianValue["ProjectSpecification"].ModifiedBy = user.GetId<int>();
                processSpecXianValue["ProjectSpecificationChangeSummary"].Value = changeHistory.ToString();
                processSpecXianValue["ProjectSpecificationChangeSummary"].ModifiedBy = user.GetId<int>();


                await UpdateProcessModifiedAndVersion(db, user, (int)stepXian.Owner);
                await db.SaveChangesAsync();


            }
        }

        public async Task<core.models.StepStatusInfo> RunStep(Pointer user, Pointer actionStep)
        {
            await _authorisationDAL.Demand(user,
                actionStep,
                new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                UserPermissionRoleTypeEnum.Editor,
                UserPermissionRoleTypeEnum.Executor,
                UserPermissionRoleTypeEnum.Owner);

            var stepXianId = actionStep.GetId<int>();

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXian = await GetStepXian(db, stepXianId, throwExceptionIfNotFound: true);
                var processId = (int)stepXian.Owner;

                var stepName = stepXian.Name;
                var stepsElement = new XElement("Steps", content: GetBareStepElement(stepXianId, stepName));

                var executionRootXml =
                        BuildExecutionRootElement(processId, null, DateTime.Now.ToUniversalTime(), ExecutionTypes.AdhocStep, "Step Execution", stepsElement);

                var executionId = await ScheduleExecution(db, user.GetId<int>(), executionRootXml);

                if (executionId > 0)
                {
                    var stepRunTimeData = await GetStepRunTimeData(db, processId, stepXianId);
                    return new core.models.StepStatusInfo(stepRunTimeData, actionStep);
                }
                else
                    throw new ApiException(ApiExceptionType.SystemError, ApiExceptionReason.FailedToExecute, $"Failed to execute {actionStep.GetId<int>()} action step");
            }
        }

        public async Task<IEnumerable<StepDependency>> GetStepDependency(Pointer user, IEnumerable<int> stepIds)
        {
            using (var db = new SqlConnection(_dalConnector.GetConnection()))
            {
                if (stepIds.Any() == false)
                    return new StepDependency[0];

                var sqlCommand = new SqlCommand($"SELECT XianID, Owner FROM [dbo].Xian WHERE XianID IN ({string.Join(",", stepIds)}) AND DateDeleted IS NULL", db);
                db.Open();

                var reader = await sqlCommand.ExecuteReaderAsync();
                var stepProcessIds = await GetStepAndProcessIds(reader);

                if (stepProcessIds != null && stepProcessIds.Count() == 0)
                    throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.StepNotFound, $"Supplied step might not exist anymore. Please supply valid existing step");

                foreach (var processId in stepProcessIds.Select(s => s.ProcessId).Distinct())
                {
                    await _authorisationDAL.Demand(user, new Pointer(EntityType.Process, processId), new UserRoleEnum[] { UserRoleEnum.Subscriber, UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin }, UserPermissionRoleTypeEnum.Reader, UserPermissionRoleTypeEnum.Editor, UserPermissionRoleTypeEnum.Executor, UserPermissionRoleTypeEnum.Owner);
                }

                sqlCommand = new SqlCommand($"SELECT xv.XianID, xv.Value FROM [dbo].XianValues xv, [dbo].XianProperties xp" +
                                                    $" WHERE xv.PropertyID = xp.PropertyID AND xp.XianTypeID = 1 AND " +
                                                            $"xp.Name = 'ProjectSpecification' AND xv.XianID IN ({string.Join(",", stepProcessIds.Select(sp => sp.ProcessId))})", db);
                reader = await sqlCommand.ExecuteReaderAsync();
                var processXmls = GetProcessIdAndXmls(reader);

                var stepDependencies = new List<StepDependency>();

                foreach (var stepProcessId in stepProcessIds)
                {
                    if (processXmls.ContainsKey(stepProcessId.ProcessId) && !string.IsNullOrWhiteSpace(processXmls[stepProcessId.ProcessId]))
                    {
                        var processXml = XElement.Parse(processXmls[stepProcessId.ProcessId]);
                        var stepPointers = GetStepPointers(processXml);
                        var stepArray = stepPointers.Select(s => s.GetId<int>()).ToArray();

                        var index = System.Array.IndexOf(stepArray, stepProcessId.StepXianId);
                        // When index is zero or less than zero, it is considered the first step in the process
                        // No dependency displayed for the first step in the process
                        if (index > 0)
                        {
                            var priorStepXianId = (index > 0) ? stepArray[index - 1] : (int?)null;
                            var priorStep = GetPriorStep(stepPointers.ToArray(), processXml, index - 1);

                            var stepXElement = processXml.XPathSelectElement("//Step[@StepXianID='" + stepProcessId.StepXianId.ToString() + "']");
                            if (stepXElement != null)
                            {
                                stepDependencies.Add(new StepDependency(stepXElement, stepPointers, priorStep));
                            }
                        }
                    }
                }

                var fileDependencies = stepDependencies.Where(d => d.DependencyExpression
                                                                    .Variables
                                                                    .Where(e => !string.IsNullOrWhiteSpace((e.Value as Dependency).XianPropertyReferenceRel))
                                                                    .Any());
                foreach (var fileDependency in fileDependencies)
                {
                    foreach (Dependency dependency in fileDependency.DependencyExpression.Variables.Values)
                    {
                        if (string.IsNullOrWhiteSpace(dependency.XianPropertyReferenceRel))
                            continue;

                        int xianId, propertyId;
                        SharedXianFunctions.SplitXianPropertyReference(dependency.XianPropertyReferenceRel, out xianId, out propertyId);

                        sqlCommand = new SqlCommand($"SELECT Value FROM [dbo].XianValues WHERE XianID = {xianId} AND PropertyID = {propertyId}", db);
                        var fileXianId = (await sqlCommand.ExecuteScalarAsync())?.ToString();

                        if (string.IsNullOrWhiteSpace(fileXianId))
                            dependency.Dependent = null;
                        else
                            dependency.Dependent = new Pointer(EntityType.File, fileXianId);
                    }
                }

                return stepDependencies;
            }
        }
        public async Task ReleaseStepDependency(Pointer user, Pointer step)
        {
            await _authorisationDAL.Demand(user, step,
                new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                                   UserPermissionRoleTypeEnum.Executor,
                                   UserPermissionRoleTypeEnum.Editor,
                                   UserPermissionRoleTypeEnum.Owner);

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXian = await (from sx in db.Xians
                                      where sx.DateDeleted == null &&
                                            sx.XianID == step.GetId<int>()
                                      select sx)
                                     .SingleOrDefaultAsync();

                if (stepXian == null)
                    throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.StepNotFound, $"Supplied step {step} is not found");

                var process = new Pointer(EntityType.Process, (int)stepXian.Owner);
                
                var executionIds = await GetRunningExecutions(db, process.GetId<int>());
                var strExecutionIds = executionIds.ToCommaSeperatedString();

                await SP_ExecutionReleaseDependency(user, step, strExecutionIds);
            }
        }

        public async Task UpdateStepEnabledStatus(Pointer user, Pointer step, bool enabled)
        {
            await _authorisationDAL.Demand(user, step,
                new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                                   UserPermissionRoleTypeEnum.Editor,
                                   UserPermissionRoleTypeEnum.Owner);

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXian = db.Xians.SingleOrDefault(a => a.XianID == step.GetId<int>());
                if (stepXian!=null)
                {
                    var process = new Pointer(EntityType.Process, (int)stepXian.Owner);

                    var processXianValues = await GetXianValues(db, process.GetId<int>(), new[] { "ProjectSpecificationChangeSummary", "ProjectSpecification" });

                    var processSpecValue = processXianValues["ProjectSpecification"];

                    var processXml = XElement.Parse(processXianValues["ProjectSpecification"].Value);
                    var changeHistory = GetEmptyLastChangeSummary();
                    changeHistory.Add(GetCustomChangeElement($"Step {step.GetId<int>()}", $"Step <b>{stepXian.Name} has been {(enabled ? "enabled" : "disabled")}"));

                    var stepXml = processXml.XPathSelectElement("//Step[@StepXianID='" + step.GetId<int>() + "']");
                    stepXml.SetAttributeValue("Enabled", enabled);

                    processXianValues["ProjectSpecificationChangeSummary"].Value = changeHistory.ToString();
                    processXianValues["ProjectSpecificationChangeSummary"].ModifiedBy = user.GetId<int>();
                    processXianValues["ProjectSpecification"].Value = processXml.ToString();
                    processXianValues["ProjectSpecification"].ModifiedBy = user.GetId<int>();

                    await db.SaveChangesAsync();

                }
            }
        }
        public async Task RestartStep(Pointer user, Pointer step)
        {
            await _authorisationDAL.Demand(user, step,
                new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin },
                                   UserPermissionRoleTypeEnum.Executor,
                                   UserPermissionRoleTypeEnum.Owner);

            await SP_RestartStep(user, step);
        }
        public async Task<StepDependency> SaveStepDependency(Pointer user, StepDependency stepDependency)
        {
            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXianId = stepDependency.ParentStep.GetId<int>();

                var processId = (await GetStepXian(db, stepXianId, throwExceptionIfNotFound: true)).Owner;

                var process = new Pointer(EntityType.Process, (int)processId);
                var ownerXian = db.Xians.Single(a => a.XianID == (int)processId);

                if (ownerXian.Flags == (int)XianFlags.IsProcessRun)
                    process = new Pointer(EntityType.ProcessRun, (int)processId);

                await _authorisationDAL.Demand(user, process, new UserRoleEnum[] { UserRoleEnum.Subscriber, UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin }, UserPermissionRoleTypeEnum.Editor, UserPermissionRoleTypeEnum.Owner);

                var processXv = await GetXianValue(db, processId.Value, "ProjectSpecification");
                var processSpecs = processXv.Value;
                var processXml = XElement.Parse(processSpecs);
                var stepPointers = GetStepPointers(processXml).ToArray();

                var index = System.Array.IndexOf(stepPointers, stepDependency.ParentStep);
                var priorStep = GetPriorStep(stepPointers, processXml, index - 1);

                if (stepDependency.DependencyExpression != null)
                {
                    foreach (var keyValuePair in stepDependency.DependencyExpression.Variables)
                    {
                        var dependency = keyValuePair.Value as Dependency;

                        if (dependency != null &&
                            dependency.SubType == SubType.PriorStep)
                        {
                            if (priorStep != null)
                            {
                                dependency.Dependent = priorStep;
                            }
                            else
                            {
                                throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.InvalidParameters, $"There is no previous enabled step found. Either explicitly specify dependent step or have enabled step before this step.");
                            }
                        }

                        if (dependency != null &&
                            dependency.Dependencytype == DependencyType.XianUpdated)
                        {
                            var fileId = dependency.Dependent.GetId<int>();
                            var xianFileRecord = await db.XianFiles.SingleOrDefaultAsync(xf => xf.XianID == fileId);
                            if (xianFileRecord != null)
                                dependency.XianPropertyReferenceRel = xianFileRecord.OwningStepPropertyReference;
                        }
                    }
                }

                var stepXElement = processXml.XPathSelectElement("//Step[@StepXianID='" + stepXianId.ToString() + "']");

                var stepDependencyToUpdate = new StepDependency(stepXElement, stepPointers, priorStep)
                {
                    DependencyExpression = stepDependency.DependencyExpression,
                    SkipDependency = (stepDependency.SkipDependencyExpression != null),
                    SkipDependencyExpression = stepDependency.SkipDependencyExpression
                };

                stepXElement = stepDependencyToUpdate.Serialise();

                processXv.Value = processXml.ToString();
                processXv.ModifiedBy = user.GetId<int>();

                await UpdateXianValues(db, new[] { processXv });

                return stepDependencyToUpdate;
            }
        }

        public async Task<IEnumerable<Pointer>> GetStepsUsingFileOrTable(Pointer user, Pointer process, Pointer item, int? loggedInUserUTCOffsetInMinutes)
        {
            await _authorisationDAL.Demand(user, process,
                            new[] { UserRoleEnum.ClientAdmin, UserRoleEnum.Designer, UserRoleEnum.SVXAdmin, UserRoleEnum.Subscriber },
                            UserPermissionRoleTypeEnum.Reader, UserPermissionRoleTypeEnum.Executor, UserPermissionRoleTypeEnum.Owner, UserPermissionRoleTypeEnum.Editor);

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var processId = process.GetId<int>();
                var strProcessXml = await GetXianValue<string>(db, processId, "ProjectSpecification");
                var processXml = XElement.Parse(strProcessXml);
                var stepEntities = GetStepPointersFromProcessExceptSpecificStepTypes(processXml);

                var stepXianIds = stepEntities.Select(s => s.Pointer.GetId<int>());
                var stepXianAndParametersXianIds = await GetStepParametersXianIds(db, stepXianIds);
                var stepParametersXianIds = stepXianAndParametersXianIds.Values.ToList();

                var itemId = item.GetId<int>();
                var searchString = string.Empty;
                List<Pointer> connectorStepXianIdListThatLinkedToFile = new List<Pointer>();

                if (item.GetEntityType() == EntityType.File)
                {
                    var fileXian = await (from x in db.Xians
                                          join xf in db.XianFiles on x.XianID equals xf.XianID
                                          join xp in db.XianProperties on xf.FilePropertyId equals xp.PropertyID
                                          where x.DateDeleted == null &&
                                                x.XianTypeID == XianSystemTypes[XianTypeNames.File] &&
                                                x.XianID == item.GetId<int>()
                                          select new
                                          {
                                              x.XianID,
                                              x.Owner,
                                              xf.OwningStepPropertyReference,
                                              ActionStepXianId = xf.StepXianId,
                                              DataStepXianId = xp.StepXianId
                                          })
                      .SingleOrDefaultAsync();

                    if (fileXian == null)
                        throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.FileNotFound, $"Supplied file id {item.GetId<int>()} does not exist");

                    var fileOwningStepParametersXianId = stepXianAndParametersXianIds.Where(s => s.Key == (fileXian.ActionStepXianId ?? fileXian.DataStepXianId).Value);
                    if (fileXian.Owner != null &&
                        fileXian.Owner.Value != processId)
                    {
                        searchString = $"XianPropertyReferenceAbs=\"{fileXian.OwningStepPropertyReference}\"";
                    }
                    else
                    {
                        searchString = $"XianPropertyReferenceRel=\"{fileXian.OwningStepPropertyReference}\"";
                    }

                    int filePropertyId = int.Parse(fileXian.OwningStepPropertyReference.Split('.')[1]);
                    connectorStepXianIdListThatLinkedToFile = await (from o in db.ConnectorStepHttpApiOutput
                                                                        join a in db.ConnectorStepHttpApis on o.ConnectorStepHttpApiID equals a.ConnectorStepHttpApiID
                                                                        where o.StepPropertyID == filePropertyId
                                                                        select new Pointer(EntityType.ConnectorStep, a.ConnectorStepID)).ToListAsync();
                }
                else
                {
                    searchString = $"XianReferenceAbs=\"{itemId}\"";
                }

                var stepParametersXianIdsThatLinkedToFileOrTable = await (from xv in db.XianValues
                                                                          join xp in db.XianProperties on xv.PropertyID equals xp.PropertyID
                                                                          where stepParametersXianIds.Contains(xv.XianID) &&
                                                                                ((xp.DataType == "XML" && xv.Value.Contains(searchString)) ||
                                                                                 (xp.DataType == "XianReferenceRel" && xv.Value == itemId.ToString()))
                                                                          select xv.XianID)
                                                                   .ToListAsync();

                if (item.GetEntityType() == EntityType.ManagedTable ||
                    item.GetEntityType() == EntityType.VirtualTable)
                {
                    var tableName = await (from x in db.Xians
                                           where x.XianID == itemId &&
                                                 x.DateDeleted == null
                                           select x.Name)
                                           .SingleAsync();

                    var stepPointersThatReferToTables = GetStepPointersFromProcessExceptSpecificStepTypes(processXml, "SXDC","XDC","MailMerge", "StepPropertyCleaner", "XianPropertyCopier", "SendMail",
                                                                                                                      "RenameRunName", "SendSMS", "EmailConfirmation", "ConvertFileFormat", "DecisionControl",
                                                                                                                      "FileFetching", "FileSending", "RunAnotherProcess", "RequestFilesViaEmail", 
                                                                                                                      "RichTextNotes", "RunExecutable");

                    var stepXianIdsThatReferToTables = stepPointersThatReferToTables.Select(s => s.Pointer.GetId<int>());
                    var stepParamXianIdsThatReferToTables = stepXianAndParametersXianIds.Where(s => stepXianIdsThatReferToTables.Contains(s.Key)).Select(s => s.Value);

                    var xmlThatMayContainTableReferences = (await (from xv in db.XianValues
                                                                   join xp in db.XianProperties on xv.PropertyID equals xp.PropertyID
                                                                   where stepParamXianIdsThatReferToTables.Contains(xv.XianID) &&
                                                                         xp.DataType == "XML" &&
                                                                         !string.IsNullOrEmpty(xv.Value)
                                                                   select new { xianId = xv.XianID, value = xv.Value })
                                                        .ToArrayAsync())
                                                        .Select(x => (x.xianId, x.value));

                    (List<(int xianId, string value)> runExcelCalcXmls,
                     List<(int xianId, string value)> instructionSqlXmls,
                     List<int> tablesUsedInProcess) = ExtractSqlXmlsAndTablesUsed(xmlThatMayContainTableReferences);

                    foreach (var (xianId, value) in runExcelCalcXmls)
                    {
                        var tablesUsedInCurrentStep = GetTablesUsedInRunExcelCalc(value);
                        if (tablesUsedInCurrentStep != null && tablesUsedInCurrentStep.Contains(itemId) &&
                            !stepParametersXianIdsThatLinkedToFileOrTable.Contains(xianId))
                            stepParametersXianIdsThatLinkedToFileOrTable.Add(xianId);
                    }

                    foreach (var (xianId, value) in instructionSqlXmls)
                    {
                        var tablesUsedInCurrentStep = await GetTablesUsedInInstructionXml(user, db, value, loggedInUserUTCOffsetInMinutes);
                        if (tablesUsedInCurrentStep != null && tablesUsedInCurrentStep.Contains(tableName, StringComparer.InvariantCultureIgnoreCase) &&
                            !stepParametersXianIdsThatLinkedToFileOrTable.Contains(xianId))
                            stepParametersXianIdsThatLinkedToFileOrTable.Add(xianId);
                    }
                }

                var results = stepXianAndParametersXianIds
                    .Where(sx => stepParametersXianIdsThatLinkedToFileOrTable.Contains(sx.Value));

                return (from sx in stepEntities
                        join spx in results on sx.Pointer.GetId<int>() equals spx.Key
                        select sx.Pointer
                        ).Union(
                            from se in stepEntities
                            where connectorStepXianIdListThatLinkedToFile.Contains(se.Pointer)
                            select se.Pointer);
            }
        }
       
        public async Task SaveStepRuntimeSettings(Pointer user, StepRunTimeSettings stepCore)
        {
            var clientId = user.GetId<int>();
            var stepXianId = stepCore.Pointer.GetId<int>();
            await _authorisationDAL.Demand(user, stepCore.Pointer, new UserRoleEnum[] { UserRoleEnum.Designer, UserRoleEnum.ClientAdmin, UserRoleEnum.SVXAdmin }, UserPermissionRoleTypeEnum.Editor, UserPermissionRoleTypeEnum.Owner);

            using (var db = new ClientDbContext(_dalConnector.GetConnection()))
            {
                var stepXian = await GetStepXian(db, stepXianId, throwExceptionIfNotFound: true);

                var stepParametersXianId = await GetXianValue<int>(db, stepXianId, "StepParametersXianID");

                using (var transactionScope = await db.Database.BeginTransactionAsync())
                {
                    await UpdateStepRuntimeSettings(db, clientId, stepXian, stepParametersXianId, stepCore);

                    transactionScope.Commit();
                }
            }
        }
        #endregion Interface Methods

        #region Private Methods
        private Dictionary<int, string> GetProcessIdAndXmls(SqlDataReader reader)
        {
            var processXmls = new Dictionary<int, string>();

            while (reader.Read())
            {
                if (!processXmls.ContainsKey(reader.GetReaderValue<int>("XianID")))
                    processXmls.Add(reader.GetReaderValue<int>("XianID"), reader.GetReaderValue<string>("Value"));
            }
            reader.Close();

            return processXmls;
        }
        private async Task<List<StepProcessId>> GetStepAndProcessIds(SqlDataReader reader)
        {
            var stepProcessIds = new List<StepProcessId>();

            while (await reader.ReadAsync())
            {
                stepProcessIds.Add(new StepProcessId() { ProcessId = reader.GetReaderValue<int>("Owner"), StepXianId = reader.GetReaderValue<int>("XianID") });
            }
            reader.Close();

            return stepProcessIds;
        }
        private async Task SP_ExecutionReleaseDependency(Pointer user, Pointer step, string strExecutionIds)
        {
            using (var connection = new SqlConnection(_dalConnector.GetConnection()))
            {
                var userIdParameter = GetSqlParameter("@ClientID", user.GetId<int>().GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input);
                var execIdsParameter = GetSqlParameter("@ListOfExecutionIDs", strExecutionIds, SqlDbType.NVarChar, ParameterDirection.Input, size: -1);
                var stepIdParameter = GetSqlParameter("@StepID", step.GetId<int>(), SqlDbType.Int, ParameterDirection.Input);

                var onFailureMessageParameter = GetSqlParameter("@OnFailureMessage", System.DBNull.Value, SqlDbType.NVarChar, ParameterDirection.Output, size: -1);

                using (var command = new SqlCommand(Procedures.ExecutionReleaseDependency, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add(userIdParameter);
                    command.Parameters.Add(execIdsParameter);
                    command.Parameters.Add(stepIdParameter);
                    command.Parameters.Add(onFailureMessageParameter);
                    _logger.LogDebug(Properties.Resources.exec_sp, command.CommandText, GetSpParamsAsString(command));

                    connection.Open();

                    await command.ExecuteNonQueryAsync();
                }

                if (onFailureMessageParameter.Value != null &&
                    string.IsNullOrWhiteSpace(onFailureMessageParameter.Value.ToString()) == false)
                    throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.FailedToCreateActionStep, $"Failed to release step dependency [{step}]. Error message was [{onFailureMessageParameter.Value.ToString()}]");

                connection.Close();
            }
        }
        private async Task SP_RestartStep(Pointer user, Pointer step)
        {
            var stepXianId = step.GetId<int>();
            using (var connection = new SqlConnection(_dalConnector.GetConnection()))
            {
                var userIdParameter = GetSqlParameter("@ClientID", user.GetId<int>().GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input);
                var stepIdParameter = GetSqlParameter("@StepID", stepXianId, SqlDbType.Int, ParameterDirection.Input);

                var onFailureMessageParameter = GetSqlParameter("@OnFailureMessage", System.DBNull.Value, SqlDbType.NVarChar, ParameterDirection.Output, size: -1);

                using (var command = new SqlCommand(Procedures.StepRestartRun, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add(userIdParameter);
                    command.Parameters.Add(stepIdParameter);
                    command.Parameters.Add(onFailureMessageParameter);
                    _logger.LogDebug(Properties.Resources.exec_sp, command.CommandText, GetSpParamsAsString(command));

                    connection.Open();

                    await command.ExecuteNonQueryAsync();
                }

                if (onFailureMessageParameter.Value != null &&
                    string.IsNullOrWhiteSpace(onFailureMessageParameter.Value.ToString()) == false)
                    throw new ApiException(ApiExceptionType.UserError, ApiExceptionReason.FailedToCreateActionStep, $"Failed to restart step [{step}]. Error message was [{onFailureMessageParameter.Value.ToString()}]");

                connection.Close();
            }
        }
        private async Task UpdateStepRuntimeSettings(ClientDbContext db, int clientId, StepXian stepXian, int stepParametersXianId, StepRunTimeSettings stepCore)
        {
            // It is valid that user has not supplied anything to update;
            if (stepCore == null)
                return;

            if (!string.IsNullOrEmpty(stepCore.Name))
                await UpdateStepName(db, clientId, stepXian, stepParametersXianId, stepCore.Name);

            if (stepCore.Description != null)
                await UpdateStepDescription(db, stepXian.XianID, stepCore.Description);

            if (stepCore.SuppressAlerts != null)
                await UpdateStepSuppressAlerts(db, stepXian.XianID, (bool)stepCore.SuppressAlerts);

            if (!string.IsNullOrEmpty(stepCore.ExecutionElapsedTimeLimitAction))
                await UpdateStepExecutionElapsedTimeLimitAction(db, stepXian.XianID, stepCore.ExecutionElapsedTimeLimitAction);

            if (stepCore.ExecutionElapsedTimeLimitInSecs != null)
                await UpdateStepExecutionElapsedTimeLimitInSecs(db, stepXian.XianID, (int)stepCore.ExecutionElapsedTimeLimitInSecs);

            if (stepCore.Notes != null)
                await UpdateStepNotes(db, clientId, stepXian.XianID, stepXian.Owner.Value, stepCore.Notes);
        }
        private async Task UpdateStepNotes(ClientDbContext db, int userId, int stepXianId, int processId, TokenisedString notes)
        {
            var processXianValues = await GetXianValues(db, processId, new[] { "ProjectSpecification" });
            var processXml = XElement.Parse(processXianValues["ProjectSpecification"].Value);
            var stepXElement = processXml.XPathSelectElement("//Step[@StepXianID='" + stepXianId.ToString() + "']");
            if (stepXElement != null)
            {
                var notesElement = stepXElement.Element("Notes");
                if (notesElement == null)
                {
                    notesElement = new XElement("Notes");
                    stepXElement.Add(notesElement);
                }

                if (string.IsNullOrWhiteSpace(notes.Value))
                {
                    notesElement.Value = string.Empty;
                }
                else
                {
                    notesElement.Value = (await GetXmlFromTokenisedString(db, processId, notes.Variables, notes.Value)).ToString();
                }

                processXianValues["ProjectSpecification"].Value = processXml.ToString();
                processXianValues["ProjectSpecification"].ModifiedBy = userId;

                await db.SaveChangesAsync();
            }
            else
            {
                // ToDo - What to do when StepXian and StepParameterXian reference for Step exists in Xian table, but it is not part of Process specification xml
            }
        }
        private async Task UpdateStepName(ClientDbContext db, int clientId, StepXian stepXian, int stepParametersXianId, string newName)
        {
            var oldName = stepXian.Name;

            var stepParametersXian = await db.Xians.SingleAsync(x => x.XianID == stepParametersXianId && x.DateDeleted == null);
            stepXian.Name = stepXian.Name.Substring(0, stepXian.Name.IndexOf(' ')+1) + newName;
            stepParametersXian.Name = stepXian.Name;

            var xvProcessSpecification = await GetXianValue(db, stepXian.Owner.Value, "ProjectSpecification");
            var processSpecXml = XElement.Parse(xvProcessSpecification.Value);

            var stepToUpdate = processSpecXml.Elements("Operation")
                          .Elements("Step")
                          .SingleOrDefault(s => s.Attribute("StepXianID").Value == stepXian.XianID.ToString());
            stepToUpdate.SetAttributeValue("Name", newName);

            xvProcessSpecification.Value = processSpecXml.ToString();
            xvProcessSpecification.ModifiedBy = clientId;

            var xvProjectSpecificationChangeSummary = await db.XianValues.SingleAsync(xv => xv.XianID == stepXian.Owner.Value && xv.XianProperty.Name == "ProjectSpecificationChangeSummary");
            xvProjectSpecificationChangeSummary.Value = GetStepNameChangeSummary(stepXian.XianID, oldName, newName).ToString();
            xvProjectSpecificationChangeSummary.ModifiedBy = clientId;
            await db.SaveChangesAsync();
        }
        private XElement GetStepNameChangeSummary(int stepXianId, string oldName, string newName)
        {
            // var changeElement = GetChangeElement($"&lt;b&gt; {newName} &lt;/b&gt;", $"renamed from &ltb&gt;{oldName}&lt;/b&gt;");
            var stdChange = GetChangeElement($"<b>{newName}</b>", $"renamed from <b>{oldName}</b>");
            stdChange.Add(new XAttribute("StepXianID", stepXianId));

            var lastChangeSummary = GetEmptyLastChangeSummary();
            lastChangeSummary.Add(stdChange);
            return lastChangeSummary;
        }
        private async Task UpdateStepExecutionElapsedTimeLimitInSecs(ClientDbContext db, int stepXianId, int timeLimitInSecs)
        {
            var maxExecutionElapsedTimeLimitInSecs =await GetXianValue<int>(db, stepXianId, "MaxExecutionElapsedTimeLimitInSecs");
            if (timeLimitInSecs> maxExecutionElapsedTimeLimitInSecs)
            {
                throw new ApiException(ApiExceptionReason.InvalidParameters, "Execution elapse time limit exceeds the maximum that can be set in minutes(" + maxExecutionElapsedTimeLimitInSecs/60+")");
            }

            var xvElapsedTimeLimitAction = await GetXianValue(db, stepXianId, "ExecutionElapsedTimeLimitInSecs");
            xvElapsedTimeLimitAction.Value = timeLimitInSecs.ToString();
            await db.SaveChangesAsync();
        }
        private async Task UpdateStepExecutionElapsedTimeLimitAction(ClientDbContext db, int stepXianId, string action)
        {
            var xvElapsedTimeLimitAction = await GetXianValue(db, stepXianId, "ExecutionElapsedTimeLimitAction");
            xvElapsedTimeLimitAction.Value = action;
            await db.SaveChangesAsync();
        }
        private async Task UpdateStepSuppressAlerts(ClientDbContext db, int stepXianId, bool suppressAlerts)
        {
            var xvSuppressAlerts = await GetXianValue(db, stepXianId, "SuppressAlerts");
            xvSuppressAlerts.Value = suppressAlerts.ToString();
            await db.SaveChangesAsync();
        }
        private async Task UpdateStepDescription(ClientDbContext db, int stepXianId, string description)
        {
            var xvDescription = await GetXianValue(db, stepXianId, "Description");
            xvDescription.Value = description;
            await db.SaveChangesAsync();
        }

        //The stepDependancy in XML of process if it has type PriorStep, must be updated with the true prior step if the step has moved
        //this is to handle it generically (ie always update priorstep properly).
        private XElement RefreshStepDependancyInXmlForPriorTypes(XElement processXml)
        {
            List<XElement> stepsList = new List<XElement>();
            foreach (var groupXML in processXml.Elements("Operation"))
            {
                var steps = groupXML.Elements("Step").ToArray();
                for (int i = 0; i < steps.Length; i++)
                {
                    stepsList.Add(steps[i]);
                }
            }
            for (int i = 0; i < stepsList.Count; i++)
            {
                //we assume any of these values could be corrupted, so check them carefully.
                var executionDependancies = stepsList[i].Element("ExecutionDependencies");
                if (executionDependancies != null)
                {
                    foreach (var executionDependancy in executionDependancies.Elements("ExecutionDependency"))
                    {
                        if (executionDependancy.Attribute("SubType") != null && executionDependancy.Attribute("SubType").Value == "PriorStep" && i > 0)
                        {
                            executionDependancy.Attribute("ID").Value = stepsList[i - 1].Attribute("StepXianID").Value;
                        }
                    }
                }
            }
            return processXml;

        }
        #endregion Private Methods

        private class StepProcessId
        {
            public int ProcessId { get; set; }
            public int StepXianId { get; set; }
        }
    }
}
