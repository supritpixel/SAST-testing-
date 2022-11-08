using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using solvexia.svx.server.lib.utilities;
using svx.lib.dalconnector;
using svx.lib.sendemail;
using System;
using System.Threading.Tasks;

namespace svx.stepengine.notification.email
{
    public class EmailNotification : IEmailNotification
    {
        #region Declarations
        private readonly IDALConnector _dalConnector;
        private readonly ILogger<EmailNotification> _logger;
        private const int _xianRunFlag = 128;
        #endregion Declarations

        #region LifeCycle
        public EmailNotification(
            IDALConnector dalConnector,
            ILogger<EmailNotification> logger)
        {
            _dalConnector = dalConnector;
            _logger = logger;

        }
        #endregion LifeCycle

        #region Interface Methods
        public async Task SendEmailToRunningUser(int clientID, int processID, string subject, string htmlBody)
        {
            using var conn = new SqlConnection(_dalConnector.GetConnection());
            var query = "SELECT [dbo].[fXianProperty_ReadValue](@XianID1, @PropertyName1) AS Email, [dbo].[fXianProperty_ReadValue](@XianID2, @PropertyName2) AS FirstName;" +
                                               "SELECT XianID, Owner, Flags FROM Xian WHERE XianID = @ProcessId AND XianTypeID = (SELECT XianID FROM XianSystemTypes WHERE Name = 'Process')";
            using var command = new SqlCommand(query, conn);

            command.Parameters.AddWithValue("@XianID1", clientID);
            command.Parameters.AddWithValue("@PropertyName1", "Email");

            command.Parameters.AddWithValue("@XianID2", clientID);
            command.Parameters.AddWithValue("@PropertyName2", "FirstName");

            command.Parameters.AddWithValue("@ProcessId", processID);

            _logger.LogDebug(Properties.Resources.sql, query);
            conn.Open();

            using var reader = await command.ExecuteReaderAsync();
            reader.Read();

            var toEmailAddress = reader.GetString(0);
            var firstName = reader.GetString(1);

            reader.NextResult();
            reader.Read();

            var runId = reader.GetInt32(0);
            var runProcessId = reader.GetInt32(1);
            var xianFlags = reader.GetInt32(2);

            _logger.LogDebug(Properties.Resources.rows_returned, 2);
            var isRun = (runId != runProcessId &&
                         (xianFlags & _xianRunFlag) > 0);

            htmlBody = htmlBody.Replace("#FirstName#", firstName);
            htmlBody = htmlBody.Replace("#ProcessRunLink#", GenerateLinkUrl(runProcessId, runId, isRun));

            using var svxMailer = new Mailer(logger: _logger,
                                       smtpHost: RuntimeSettings.Get("SMTPHost", "NotSet"),
                                       smtpPort: RuntimeSettings.Get("SMTPHostPort", 2525),
                                       userName: RuntimeSettings.Get("SMTPUserID", "NotSet"),
                                       password: RuntimeSettings.Get("SMTPPassword", "NotSet"),
                                       smtpTimeoutInSecondsPerMB: RuntimeSettings.Get("SMTPTimeoutInSecondsPerMBAttached", 45),
                                       smtpMaxTimeoutInSeconds: RuntimeSettings.Get("SMTPMaxTimeoutInSeconds ", 1800));

            svxMailer.SendEmailSync(ToRecipients: toEmailAddress,
                                    CCRecipients: null,
                                    SenderEmail: "administrator@solvexia.com",
                                    Subject: subject,
                                    BodyIsHTML: true,
                                    Body: htmlBody,
                                    Attachments: null);
        }
        #endregion Interface Methods

        #region Private Methods
        private string GenerateLinkUrl(int processId, int? runId, bool isRun)
        {
            var baseUrl = RuntimeSettings.GetButDontCreate("EmailConfirmation", "baseUrl", "NotKnown");

            if (isRun)
                return $"{baseUrl}/web/nsv/processrun/{runId}/info";

            return $"{baseUrl}/web/nsv/process/{processId}";
        }
        #endregion Private Methods
    }
}
