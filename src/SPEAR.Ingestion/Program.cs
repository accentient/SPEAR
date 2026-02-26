using Aspose.Email.Mapi;
using Aspose.Email.Mime;
using Aspose.Email.Storage.Pst;
using Force.Crc32;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace EmailToSQL
{
  class Program
  {
    private static string FILES_PATH;
    private static string DELETEDUPES_FILE;

    private static string CONN_STRING;
    private static string TEMP_PATH;
    private static string ERROR_PATH;
    private static bool SKIP_CALENDAR;
    private static bool TRUNCATE_TABLES;
    private static bool EXTRACT;
    private static string EXTRACT_PATH;

    private static SqlConnection connection;
    private static int errors;

    private static DateTime lastDate;

    static void TruncateTables()
    {
      string sqlCommand = "ALTER TABLE EmailAttachment DROP CONSTRAINT IF EXISTS FK_Email; TRUNCATE TABLE EmailAttachment; TRUNCATE TABLE Email; ALTER TABLE EmailAttachment ADD CONSTRAINT FK_Email FOREIGN KEY (EmailID) REFERENCES Email(EmailID)";
      // Create a command to execute the SQL statement
      using (var command = new SqlCommand(sqlCommand, connection))
      {
        // Execute the command
        command.ExecuteNonQuery();
        Trace.WriteLine("");
        Trace.WriteLine("[Email] table truncated");
        Trace.WriteLine("[EmailAttachment] table truncated");
        Trace.WriteLine("");
      }
    }

    static void DeleteTempFiles()
    {
      DirectoryInfo dir = new DirectoryInfo(TEMP_PATH);
      foreach (FileInfo file in dir.GetFiles())
      {
        file.Delete();
      }
    }

    static void DeleteErrorFiles()
    {
      DirectoryInfo dir = new DirectoryInfo(ERROR_PATH);
      foreach (FileInfo file in dir.GetFiles())
      {
        file.Delete();
      }
    }

    static void DeleteExtractFiles()
    {
      DirectoryInfo dir = new DirectoryInfo(EXTRACT_PATH);
      foreach (FileInfo file in dir.GetFiles())
      {
        file.Delete();
      }
    }

    static string FixDelimitters(string email)
    {
      return email.Replace("\"'", "\"").Replace("'\"", "\"");
    }

    static string GetAlternativeDate(HeaderCollection headers)
    {
      // First, look for a header with a simple date (fixing "00") year if necessary

      for (int i = 0; i < headers.Count; i++)
      {
        string value = headers[i].Trim();
        if (value.Length > 3 && value.Substring(value.Length - 3, 3) == "/00")
        {
          value = value.Replace("/00", "/2000");
          DateTime tempDate;
          if (DateTime.TryParse(value, out tempDate))
          {
            return value;
          }
        }
      }

      // Next, look for a received header

      for (int i = 0; i < headers.Count; i++)
      {

        if (headers.Keys[i].Trim().ToLower() == "received")
        {
          // https://regex101.com is your friend

          string input = headers[i].Trim().ToLower();

          // string pattern = @"(0[1-9]|[12]\d|3[01])\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})";
          // string pattern = @"(0[1-9]|[12]\d|3[01])\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})\s([0-9][0-9]+:+[0-9][0-9]+:+[0-9][0-9])\s([+-][0-9][0-9][0-9][0-9])";

          string pattern = @"([0-9]+)\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})\s([0-9][0-9]+:+[0-9][0-9]+:+[0-9][0-9])\s([+-][0-9][0-9][0-9][0-9])";
          RegexOptions options = RegexOptions.Multiline;
          foreach (Match m in Regex.Matches(input, pattern, options))
          {
            DateTime tempDate;
            if (DateTime.TryParse(m.Value, out tempDate))
            {
              return m.Value;
            }
          }
        }
      }

      // Didn't find anything

      return "";
    }

    static string GetAlternativeSender(HeaderCollection headers)
    {
      // Look for a received header

      // From header?
      string value = headers["from"];
      try
      {
        var email = new System.Net.Mail.MailAddress(value);
        return value;
      }
      catch (Exception) { }

      // Sender header?

      value = headers["sender"];
      try
      {
        var email = new System.Net.Mail.MailAddress(value);
        return value;
      }
      catch (Exception) { }

      // Reply To header?

      value = headers["reply-to"];
      try
      {
        var email = new System.Net.Mail.MailAddress(value);
        return value;
      }
      catch (Exception) { }

      return "";
    }

    static DateTime GetAlternativeDateCalendar(Aspose.Email.Mapi.MapiPropertyCollection properties)
    {
      DateTime tempDate;
      foreach (KeyValuePair<long, MapiProperty> prop in properties)
      {
        if (((prop.Value).Descriptor).CanonicalName != null)
        {
          if (((prop.Value).Descriptor).CanonicalName == "PidLidAppointmentStartWhole")
          {
            if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          }
          //if (((prop.Value).Descriptor).CanonicalName == "PidLidCommonStart") {
          //  if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          //}
          //if (((prop.Value).Descriptor).CanonicalName == "PidLidClipStart")
          //{
          //  if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          //}
          //if (((prop.Value).Descriptor).CanonicalName == "PidLidCommonEnd")
          //{
          //  if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          //}
          //if (((prop.Value).Descriptor).CanonicalName == "PidLidAppointmentEndWhole")
          //{
          //  if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          //}
          //if (((prop.Value).Descriptor).CanonicalName == "PidLidClipEnd")
          //{
          //  if (DateTime.TryParse(prop.Value.ToString(), out tempDate)) { return tempDate; };
          //}
        }
      }
      return DateTime.MinValue;
    }

    static void SaveEMLEmail(string dbx, string emlFile, Aspose.Email.MailMessage message)
    {
      try
      {
        // Define your INSERT SQL statement
        string insertEmailSql = "INSERT INTO Email (Source,SentReceived,Sender,[To],CC,BCC,Subject,BodyType,Body,BodyHtml,Attachments) VALUES (@Source,@SentReceived,@Sender,@To,@CC,@BCC,@Subject,@BodyType,@Body,@BodyHtml,@Attachments); SELECT CAST(scope_identity() AS int)";

        // Create a command to execute the SQL statement
        using (var command = new SqlCommand(insertEmailSql, connection))
        {
          // Set properties

          string from = "";
          string recipientsTo = "";
          string recipientsCC = "";
          string recipientsBCC = "";
          MailAddress tempEmail;

          // From

          if (message.From != null && message.From.Address.Trim() != "")
          {
            MailAddress.TryCreate(message.From.Address, message.From.DisplayName, out tempEmail);
            if (tempEmail != null)
            {
              from = tempEmail.ToString();
            }
          }
          else
          {
            from = GetAlternativeSender(message.Headers);
          }

          // To

          foreach (Aspose.Email.MailAddress address in message.To)
          {
            if (address.Address.Trim() == "")
            {
              if (recipientsTo == "")
                recipientsTo = address.OriginalAddressString;
              else
                recipientsTo += "; " + address.OriginalAddressString;
            }
            else
            {
              MailAddress.TryCreate(address.Address, address.DisplayName, out tempEmail);
              if (tempEmail != null)
              {
                if (recipientsTo == "")
                  recipientsTo = tempEmail.ToString();
                else
                  recipientsTo += "; " + tempEmail.ToString();
              }
            }
          }

          // CC

          foreach (Aspose.Email.MailAddress address in message.CC)
          {
            if (address.Address.Trim() != "")
            {
              MailAddress.TryCreate(address.Address, address.DisplayName, out tempEmail);
              if (tempEmail != null)
              {
                if (recipientsTo == "")
                  recipientsCC = tempEmail.ToString();
                else
                  recipientsCC += "; " + tempEmail.ToString();
              }
            }
          }

          // BCC

          foreach (Aspose.Email.MailAddress address in message.Bcc)
          {
            if (address.Address.Trim() != "")
            {
              MailAddress.TryCreate(address.Address, address.DisplayName, out tempEmail);
              if (tempEmail != null)
              {
                if (recipientsTo == "")
                  recipientsBCC = tempEmail.ToString();
                else
                  recipientsBCC += "; " + tempEmail.ToString();
              }
            }
          }

          // Bad Date?

          DateTime messageDate = message.Date;
          if (messageDate.Year < 1990)
          {
            string dateString = GetAlternativeDate(message.Headers);
            if (!DateTime.TryParse(dateString, out messageDate))
            {
              messageDate = lastDate;
            }
          }

          // Parameters

          command.Parameters.Add("@EmailID", SqlDbType.Int, 4).Direction = ParameterDirection.Output;
          command.Parameters.AddWithValue("@Source", Path.GetFileNameWithoutExtension(dbx) + "-" + Path.GetFileName(emlFile));
          command.Parameters.AddWithValue("@SentReceived", messageDate);
          command.Parameters.AddWithValue("@Sender", from);
          command.Parameters.AddWithValue("@To", FixDelimitters(recipientsTo));
          command.Parameters.AddWithValue("@CC", FixDelimitters(recipientsCC));
          command.Parameters.AddWithValue("@BCC", FixDelimitters(recipientsBCC));
          command.Parameters.AddWithValue("@Subject", message.Subject ?? "");
          command.Parameters.AddWithValue("@BodyType", message.BodyType.ToString());
          command.Parameters.AddWithValue("@Body", message.Body ?? "");
          command.Parameters.AddWithValue("@BodyHtml", message.HtmlBody ?? "");
          command.Parameters.AddWithValue("@Attachments", message.Attachments.Count);

          // Execute

          int emailID = (int)command.ExecuteScalar();

          // Save attachments

          if (message.Attachments.Count > 0)
          {
            foreach (Aspose.Email.Attachment attachment in message.Attachments)
            {
              string attachmentName = attachment.Name;

              if (attachment.Name == "" &&
                  !message.Body.ToLower().Contains("this is a receipt") &&
                  !message.Body.ToLower().Contains("the message that you sent was delivered"))
              {
                attachmentName = "Untitled.txt";

                Trace.WriteLine("");
                Trace.WriteLine("");
                Trace.WriteLine("Error: " + dbx + " / " + emlFile + " - no attachment file name and not a read receipt");
                Trace.WriteLine("");
              }

              // Trace.WriteLine(attachment.ContentDisposition.ToString() + " - " + attachment.ContentType.ToString() + " - " + attachment.TransferEncoding.ToString());
              if (attachmentName != "")
              {
                MemoryStream stream = new MemoryStream();
                attachment.Save(stream);
                stream.Position = 0;

                command.CommandText = "INSERT INTO EmailAttachment (EmailID,Name,Attachment) VALUES (@EmailID,@Name,@Attachment)";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@EmailID", emailID);
                command.Parameters.AddWithValue("@Name", attachment.Name);
                command.Parameters.AddWithValue("@Attachment", stream);
                command.ExecuteNonQuery();
              }
            }
          }

          // Save Date to use for next email

          lastDate = messageDate;
        }
      }
      catch (Exception ex)
      {
        if (ex.Message.ToLower().Contains("sqldatetime overflow"))
        {
          errors++;
          string newPath = ERROR_PATH + Path.GetFileNameWithoutExtension(dbx) + "-" + Path.GetFileName(emlFile);
          File.Copy(emlFile, newPath);

          string errorFile = ERROR_PATH + Path.GetFileNameWithoutExtension(dbx) + "-" + Path.GetFileNameWithoutExtension(emlFile) + ".log";
          using (StreamWriter writer = new StreamWriter(errorFile))
          {
            writer.WriteLine(Path.GetFileName(dbx) + " / " + Path.GetFileName(emlFile));
            writer.WriteLine();
            writer.WriteLine(ex.Message);
            writer.WriteLine();

            for (int i = 0; i < message.Headers.Count; i++)
            {
              writer.WriteLine(message.Headers.Keys[i].ToString() + " = " + message.Headers[i].ToString());
            }
            writer.Close();
          }
        }
      }
    }

    static void ReadEMLFiles(string file)
    {
      int count = 0;
      foreach (string emlFile in Directory.GetFiles(TEMP_PATH, "*.eml"))
      {
        Aspose.Email.MailMessage message = Aspose.Email.MailMessage.Load(emlFile);
        SaveEMLEmail(file, emlFile, message);
        count++;
        if (count % 100 == 0)
        {
          if (count != 100)
          {
            Trace.Write(",");
          }
          Trace.Write(count.ToString());
        }
      }
      if (count > 100)
      {
        Trace.Write(",");
      }
      if (errors == 1)
      {
        Trace.WriteLine(count.ToString() + " (" + errors.ToString() + " error)");
      }
      else
      {
        Trace.WriteLine(count.ToString() + " (" + errors.ToString() + " errors)");
      }
      errors = 0;
      lastDate = DateTime.MinValue;
    }

    static void ReadDBX(string file)
    {
      DeleteTempFiles();
      using (DBX DBX = new DBX())
      {
        int emailCount = 0;
        try
        {
          emailCount = DBX.Parse(file);
        }
        catch (Exception)
        {
          emailCount = 0;
        }
        Trace.Write(Path.GetFileName(file) + " (" + emailCount.ToString() + " items) ... ");
        if (emailCount > 0)
        {
          for (int i = 0; i < emailCount; i++)
          {
            DBX.Extract(i, TEMP_PATH + (i + 1).ToString() + ".eml");
          }
          ReadEMLFiles(file);
        }
        else
        {
          Trace.WriteLine("");
        }
      }
      DeleteTempFiles();
    }

    static void SavePSTEmail(string file, string folder, MapiMessage message)
    {
      try
      {
        // Define your INSERT SQL statement
        //string insertSql = "INSERT INTO Email (Folder, SentReceived, Sender, [To], CC, BCC, Subject, BodyType, Body, BodyHtml, BodyRtf, Attachments) VALUES (@Folder, @SentReceived, @Sender, @To, @CC, @BCC, @Subject, @BodyType, @Body, @BodyHtml, @BodyRtf, @Attachments)";
        string insertEmailSql = "INSERT INTO Email (Source,SentReceived,Sender,[To],CC,BCC,Subject,BodyType,Body,BodyHtml,Attachments) VALUES (@Source,@SentReceived,@Sender,@To,@CC,@BCC,@Subject,@BodyType,@Body,@BodyHtml,@Attachments); SELECT CAST(scope_identity() AS int)";

        // Create a command to execute the SQL statement
        using (var command = new SqlCommand(insertEmailSql, connection))
        {

          // Fix date if necessary

          DateTime emailDateTime = message.DeliveryTime;
          if (emailDateTime == DateTime.MinValue)
          {
            emailDateTime = message.ClientSubmitTime;
            if (emailDateTime == DateTime.MinValue)
            {
              emailDateTime = GetAlternativeDateCalendar(message.NamedProperties);
            }
          }

          // Set properties

          MailAddress sender = new MailAddress(message.SenderEmailAddress, message.SenderName);

          string recipientsTo = "";
          string recipientsCC = "";
          string recipientsBCC = "";

          foreach (MapiRecipient recipient in message.Recipients)
          {
            MailAddress tempEmail;
            MailAddress.TryCreate(recipient.EmailAddress, recipient.DisplayName, out tempEmail);

            if (tempEmail != null)
            {
              if (recipient.RecipientType == MapiRecipientType.MAPI_TO)
              {
                if (recipientsTo == "")
                  recipientsTo = tempEmail.ToString();
                else
                  recipientsTo += "; " + tempEmail.ToString();
              }
              else if (recipient.RecipientType == MapiRecipientType.MAPI_CC)
              {
                if (recipientsCC == "")
                  recipientsCC = tempEmail.ToString();
                else
                  recipientsCC += "; " + tempEmail.ToString();
              }
              else if (recipient.RecipientType == MapiRecipientType.MAPI_BCC)
              {
                if (recipientsBCC == "")
                  recipientsBCC = tempEmail.ToString();
                else
                  recipientsBCC += "; " + tempEmail.ToString();
              }
            }
          }

          // Parameters

          command.Parameters.Add("@EmailID", SqlDbType.Int, 4).Direction = ParameterDirection.Output;
          command.Parameters.AddWithValue("@Source", Path.GetFileNameWithoutExtension(file) + "-" + folder);
          command.Parameters.AddWithValue("@SentReceived", emailDateTime);
          command.Parameters.AddWithValue("@Sender", sender.ToString());
          command.Parameters.AddWithValue("@To", FixDelimitters(recipientsTo));
          command.Parameters.AddWithValue("@CC", FixDelimitters(recipientsCC));
          command.Parameters.AddWithValue("@BCC", FixDelimitters(recipientsBCC));
          command.Parameters.AddWithValue("@Subject", message.Subject ?? "");
          command.Parameters.AddWithValue("@BodyType", message.BodyType.ToString());
          command.Parameters.AddWithValue("@Body", message.Body ?? "");
          command.Parameters.AddWithValue("@BodyHtml", message.BodyHtml ?? "");
          command.Parameters.AddWithValue("@BodyRtf", message.BodyRtf ?? "");
          command.Parameters.AddWithValue("@Attachments", message.Attachments.Count);

          // Define parameters and their values

          //command.Parameters.AddWithValue("@Folder", folder);
          //command.Parameters.AddWithValue("@SentReceived", message.DeliveryTime);
          //command.Parameters.AddWithValue("@Sender", sender.ToString());
          //command.Parameters.AddWithValue("@To", FixDelimitters(recipientsTo));
          //command.Parameters.AddWithValue("@CC", FixDelimitters(recipientsCC));
          //command.Parameters.AddWithValue("@BCC", FixDelimitters(recipientsBCC));
          //command.Parameters.AddWithValue("@Subject", message.Subject ?? "");
          //command.Parameters.AddWithValue("@BodyType", message.BodyType.ToString());
          //command.Parameters.AddWithValue("@Body", message.Body ?? "");
          //command.Parameters.AddWithValue("@BodyHtml", message.BodyHtml ?? "");
          //command.Parameters.AddWithValue("@BodyRtf", message.BodyRtf ?? "");
          //command.Parameters.AddWithValue("@Attachments", message.Attachments.Count);

          // Execute

          int emailID = (int)command.ExecuteScalar();

          // Save attachments

          if (message.Attachments.Count > 0)
          {
            foreach (MapiAttachment attachment in message.Attachments)
            {
              string attachmentName = attachment.FileName;

              if (attachment.FileName == "" &&
                  !message.Body.ToLower().Contains("this is a receipt") &&
                  !message.Body.ToLower().Contains("the message that you sent was delivered"))
              {
                attachmentName = "Untitled.txt";

                Trace.WriteLine("");
                Trace.WriteLine("");
                Trace.WriteLine("Error: " + Path.GetFileNameWithoutExtension(file) + "-" + folder + " - no attachment file name and not a read receipt");
                Trace.WriteLine("");
              }

              // Trace.WriteLine(attachment.ContentDisposition.ToString() + " - " + attachment.ContentType.ToString() + " - " + attachment.TransferEncoding.ToString());
              if (attachmentName != "")
              {
                MemoryStream stream = new MemoryStream();
                attachment.Save(stream);
                stream.Position = 0;

                command.CommandText = "INSERT INTO EmailAttachment (EmailID,Name,Attachment) VALUES (@EmailID,@Name,@Attachment)";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@EmailID", emailID);
                command.Parameters.AddWithValue("@Name", attachment.FileName);
                command.Parameters.AddWithValue("@Attachment", stream);
                command.ExecuteNonQuery();
              }
            }
          }
        }
      }
      catch (Exception ex)
      {
        if (ex.Message.ToLower().Contains("sqldatetime overflow"))
        {
          errors++;

          string errorFile = ERROR_PATH + Path.GetFileNameWithoutExtension(file) + "-" + folder.Trim() + ".log";
          using (StreamWriter writer = new StreamWriter(errorFile))
          {
            writer.WriteLine(Path.GetFileNameWithoutExtension(file) + "-" + folder);
            writer.WriteLine();
            writer.WriteLine(ex.Message);
            writer.WriteLine();

            for (int i = 0; i < message.Headers.Count; i++)
            {
              writer.WriteLine(message.Headers.Keys[i].ToString() + " = " + message.Headers[i].ToString());
            }
            writer.Close();
          }
          Trace.WriteLine("  Error: " + ex.Message);
        }
      }
    }

    static void ReadPSTFolder(string file, PersonalStorage personalStorage, FolderInfo folderInfo)
    {
      int count = 0;
      foreach (var messageEntryId in folderInfo.EnumerateMessagesEntryId())
      {
        try
        {
          MapiMessage message = personalStorage.ExtractMessage(messageEntryId);
          SavePSTEmail(file, folderInfo.DisplayName, message);
          count++;
          if (count % 1000 == 0)
          {
            if (count != 1000)
            {
              Trace.Write(",");
            }
            Trace.Write(count.ToString());
          }
        }
        catch (Exception ex)
        {
          Trace.WriteLine("  Error: " + ex.Message + " (" + Path.GetFileName(file) + ")");
        }
      }
    }

    static void ReadPST(string file)
    {
      try
      {
        PersonalStorage personalStorage = PersonalStorage.FromFile(file);

        // Get the folders information
        FolderInfoCollection folderInfoCollection = personalStorage.RootFolder.GetSubFolders();

        // Browse through each folder to display folder name and number of messages
        foreach (FolderInfo folder in folderInfoCollection)
        {
          if (folder.DisplayName.Trim().ToLower() != "deleted items")
          {
            if (! folder.DisplayName.Trim().ToLower().Contains("calendar") || ! SKIP_CALENDAR)
            {
              Trace.Write(Path.GetFileName(file) + " / " + folder.DisplayName + " (" + folder.ContentCount.ToString() + " items) ... ");
              ReadPSTFolder(file, personalStorage, folder);
              Trace.WriteLine("");
            }
          }
        }
      }
      catch (Exception ex)
      {
        Trace.WriteLine(Path.GetFileName(file));
        Trace.WriteLine("  Error: " + ex.Message);
      }
    }

    static void ReadDataFiles()
    {
      ArrayList dataFiles = new System.Collections.ArrayList();
      foreach (string file in Directory.GetFiles(FILES_PATH, "*.dbx"))
        dataFiles.Add(file.ToString());
      foreach (string file in Directory.GetFiles(FILES_PATH, "*.pst"))
        dataFiles.Add(file.ToString());

      dataFiles.Sort();

      foreach (string file in dataFiles)
      {
        if (file.ToLower().Contains(".pst"))
          ReadPST(file);
        else if (file.ToLower().Contains(".dbx"))
          ReadDBX(file);
      }
    }

    static void LoadEmails()
    {
      // Set Aspose licenses
      Aspose.Email.License asposeLicense = new Aspose.Email.License();
      asposeLicense.SetLicense("Aspose.Total.Product.Family.lic");

      // Open DB Connection

      connection = new SqlConnection(CONN_STRING);
      connection.Open();

      // Clear database
      if (TRUNCATE_TABLES)
      {
        TruncateTables();
      }

      // Clear temporary files
      Directory.CreateDirectory(TEMP_PATH);
      DeleteTempFiles();
      DeleteErrorFiles();

      // Read DBX and PST files
      errors = 0;
      ReadDataFiles();

      // Cleanup
      connection.Close();
      DeleteTempFiles();
    }

    static void Extract()
    {
      DeleteExtractFiles();

      connection = new SqlConnection(CONN_STRING);
      connection.Open();

      SqlCommand command = new SqlCommand("SELECT EmailAttachmentID, Name, Attachment FROM EmailAttachment", connection);
      SqlDataReader reader = command.ExecuteReader();
      while (reader.Read())
      {
        try
        {
          int identity = reader.GetInt32(0);
          string filename = reader.GetString(1);

          // Strip path

          filename = Path.GetFileName(filename).Trim();

          // Valid filename?

          if (string.IsNullOrEmpty(filename))
          {
            filename = "missingfilename_" + Path.GetTempFileName();
          }
          else
          {
            if (filename.IndexOfAny(Path.GetInvalidFileNameChars()) > 0)
            {
              filename = "badfilename_" + Path.GetTempFileName();
            }
            else
            {
              // no extension?

              if (string.IsNullOrEmpty(Path.GetExtension(filename)))
              {
                // Trace.WriteLine("");
                // Trace.WriteLine(filename + " > " + filename + ".eml");
                filename += ".eml";
              }
              // else if (Path.GetExtension(filename).Length > 5 && Path.GetExtension(filename).ToLower() != ".mpeg" && Path.GetExtension(filename).ToLower() != ".html")
              else if (Path.GetExtension(filename).Length > 5)
              {
                // Trace.WriteLine(filename + " seems to be an .eml file");
                filename = Path.GetFileNameWithoutExtension(filename) + ".eml";
              }
            }
          }

          // Rename .awd to .tiff (fax)
          // rename .nws to .eml

          filename = Path.GetFileNameWithoutExtension(filename) + "-" + identity.ToString() + Path.GetExtension(filename);
          byte[] attachment = (byte[])reader.GetValue(2);
          FileStream stream = new FileStream(EXTRACT_PATH + filename, FileMode.Create);
          stream.Write(attachment, 0, attachment.Length);
          stream.Close();
        }
        catch (Exception ex)
        {
          Trace.WriteLine("");
          Trace.WriteLine("Error: " + ex.Message);
        }
      }
      connection.Close();
    }

    static void GenerateDeleteDuplicatesBat(string path, string commandFile)
    {
      Trace.WriteLine($" Generating {commandFile} to delete duplicates in {path} ...");
      Trace.WriteLine("");

      int total = 0;

      // Open the .bat file fresh
      using (var writer = new StreamWriter(commandFile, false, Encoding.Default))
      {
        writer.WriteLine("@echo off");
        writer.WriteLine("REM Duplicate file cleanup script");
        writer.WriteLine();

        var files = Directory.EnumerateFiles(path, "*.*", SearchOption.TopDirectoryOnly);
        var fileHashes = new ConcurrentDictionary<string, List<string>>();

        Parallel.ForEach(
            files,
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
            file =>
            {
              var info = new FileInfo(file);
              string quickKey = $"{info.Length}-{QuickFingerprint(file)}";

              fileHashes.AddOrUpdate(
                  quickKey,
                  _ => new List<string> { file },
                  (_, list) =>
                  {
                    lock (list)
                    {
                      if (list.Count > 0)
                      {
                        lock (writer) // thread-safe writes
                        {
                          // writer.WriteLine($"DEL \"{file}\"  REM duplicate of {list[0]}");
                          writer.WriteLine($"DEL \"{file}\"");
                        }

                        // increment total duplicates count (thread-safe)
                        Interlocked.Increment(ref total);
                      }
                      list.Add(file);
                      return list;
                    }
                  });
            });

        writer.WriteLine();
        writer.WriteLine("echo Done removing duplicates.");
        writer.WriteLine();
        writer.WriteLine("PAUSE");
      }
      Trace.WriteLine($" Total DEL statements: {total}");
    }

    #region List Duplicates
    public sealed class DuplicateGroup
    {
      public string Source { get; init; }
      public List<string> Candidates { get; init; } = new();
    }

    static List<DuplicateGroup> ListDuplicates(string path)
    {
      Trace.WriteLine($" Duplicate email files found in {path} ...");
      Trace.WriteLine("");

      int total = 0;

      var files = Directory.EnumerateFiles(path, "*.*", SearchOption.TopDirectoryOnly);
      var fileHashes = new ConcurrentDictionary<string, List<string>>();

      Parallel.ForEach(
        files,
        new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
        file =>
        {
          var info = new FileInfo(file);
          string quickKey = $"{info.Length}-{QuickFingerprint(file)}";

          fileHashes.AddOrUpdate(
            quickKey,
            _ => new List<string> { file },
            (_, list) =>
            {
              lock (list)
              {
                if (list.Count > 0)
                {
                  Trace.WriteLine($"  {file}");
                  Interlocked.Increment(ref total);
                }

                list.Add(file);
                return list;
              }
            });
        });

      // -------------------------------------------------------
      // Build grouped output + in-memory groups
      // -------------------------------------------------------
      var duplicateGroups = new List<DuplicateGroup>();
      var candidateLines = new List<string>();

      foreach (var kvp in fileHashes.OrderBy(k => k.Key))
      {
        var group = kvp.Value;

        if (group.Count < 2)
          continue;

        string source = group[0];

        var dupGroup = new DuplicateGroup
        {
          Source = source,
          Candidates = group.Skip(1).ToList()
        };

        duplicateGroups.Add(dupGroup);

        foreach (var candidate in dupGroup.Candidates)
          candidateLines.Add($"{dupGroup.Source},{candidate}");

        candidateLines.Add("");
      }

      // Write quick fingerprint matches
      string candidateFile = Path.Combine(path, "dupe-candidates.txt");
      File.WriteAllLines(candidateFile, candidateLines);

      Trace.WriteLine("");
      Trace.WriteLine($"  Total candidates: {total}");
      Trace.WriteLine($"  Candidate list written to: {candidateFile}");
      Trace.WriteLine("");

      // -------------------------------------------------------
      // Deep verification phase
      // -------------------------------------------------------
      var confirmedPairs = new List<(string Source, string Dupe)>();
      var confirmedLines = new List<string>();

      int verifyTotal = duplicateGroups.Sum(g => g.Candidates.Count);
      int verifyDone = 0;

      Trace.WriteLine("");
      Trace.WriteLine($" Starting deep verification ({verifyTotal} comparisons)...");
      Trace.WriteLine("");

      var stopwatch = System.Diagnostics.Stopwatch.StartNew();

      foreach (var group in duplicateGroups)
      {
        foreach (var candidate in group.Candidates)
        {
          verifyDone++;

          Trace.WriteLine(
            $"  [{verifyDone}/{verifyTotal}] Verifying: {Path.GetFileName(candidate)}");

          if (QuickCompare(group.Source, candidate))
          {
            confirmedPairs.Add((group.Source, candidate));
            confirmedLines.Add($"{group.Source},{candidate}");
            Trace.WriteLine("      CONFIRMED");
          }
          else
          {
            Trace.WriteLine("      rejected");
          }
        }

        if (confirmedLines.Count > 0 && confirmedLines[^1] != "")
          confirmedLines.Add("");
      }

      stopwatch.Stop();

      Trace.WriteLine("");
      Trace.WriteLine($" Deep verification completed in {stopwatch.Elapsed}");

      // Write confirmed duplicates list
      string confirmedFile = Path.Combine(path, "dupe-confirmed.txt");
      File.WriteAllLines(confirmedFile, confirmedLines);
      Trace.WriteLine($" Confirmed duplicate list written to: {confirmedFile}");

      // -------------------------------------------------------
      // Generate CMD script to move dupes into .\Dupes
      // (keeps the left-of-comma Source files)
      // -------------------------------------------------------
      string cmdFile = Path.Combine(path, "move-duplicates-to-dupes.cmd");

      var cmdLines = new List<string>
  {
    "@echo off",
    "setlocal",
    "echo Moving confirmed duplicate files to .\\Dupes ...",
    "echo.",
    "if not exist \"Dupes\" mkdir \"Dupes\"",
    "echo."
  };

      // Use relative destination folder; move only the dupe (right side).
      foreach (var (_, dupe) in confirmedPairs)
      {
        // If you ever encounter special characters, quoting handles spaces.
        cmdLines.Add($"move /y \"{dupe}\" \"Dupes\\\"");
      }

      cmdLines.Add("echo.");
      cmdLines.Add("echo Done.");
      cmdLines.Add("pause");
      cmdLines.Add("endlocal");

      File.WriteAllLines(cmdFile, cmdLines);
      Trace.WriteLine($" Move script written to: {cmdFile}");

      return duplicateGroups;
    }

    // Generates a quick fingerprint (MD5 hash) of a file by sampling data
    // from the beginning, middle, and end rather than hashing the entire file.
    static string QuickFingerprint(string filename, int sampleSize = 64 * 1024)
    {
      using (var stream = File.OpenRead(filename))
      using (var md5 = MD5.Create())
      {
        byte[] buffer = new byte[sampleSize];
        long length = stream.Length;

        int read = stream.Read(buffer, 0, buffer.Length);
        if (read > 0)
          md5.TransformBlock(buffer, 0, read, buffer, 0);

        if (length > sampleSize * 2)
        {
          long middlePos = length / 2;
          stream.Seek(middlePos - (sampleSize / 2), SeekOrigin.Begin);
          read = stream.Read(buffer, 0, buffer.Length);
          if (read > 0)
            md5.TransformBlock(buffer, 0, read, buffer, 0);
        }

        if (length > sampleSize)
        {
          stream.Seek(-sampleSize, SeekOrigin.End);
          read = stream.Read(buffer, 0, buffer.Length);
          if (read > 0)
            md5.TransformBlock(buffer, 0, read, buffer, 0);
        }

        md5.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return BitConverter.ToString(md5.Hash).Replace("-", "").ToLowerInvariant();
      }
    }

    // Performs a fast streaming byte comparison between two files.
    static bool QuickCompare(string fileA, string fileB, int bufferSize = 1024 * 1024)
    {
      var infoA = new FileInfo(fileA);
      var infoB = new FileInfo(fileB);

      if (infoA.Length != infoB.Length)
        return false;

      using var streamA = File.OpenRead(fileA);
      using var streamB = File.OpenRead(fileB);

      byte[] bufferA = new byte[bufferSize];
      byte[] bufferB = new byte[bufferSize];

      while (true)
      {
        int readA = streamA.Read(bufferA, 0, bufferA.Length);
        int readB = streamB.Read(bufferB, 0, bufferB.Length);

        if (readA != readB)
          return false;

        if (readA == 0)
          return true;

        for (int i = 0; i < readA; i++)
          if (bufferA[i] != bufferB[i])
            return false;
      }
    }
    #endregion

    #region List Count by File Type
    static void ListCountByFileType(string path)
    {
      Trace.WriteLine($" Email files found in {path} ...");
      Trace.WriteLine("");

      var files = Directory.EnumerateFiles(path, "*.*", SearchOption.TopDirectoryOnly);
      var extensionCounts = files
          .Select(f => Path.GetExtension(f).ToLowerInvariant()) // normalize extensions
          .GroupBy(ext => string.IsNullOrEmpty(ext) ? "[no extension]" : ext)
          .Select(g => new { Extension = g.Key, Count = g.Count() })
          .OrderBy(x => x.Extension)
          .ToList();
      int total = 0;
      foreach (var item in extensionCounts)
      {
        Console.WriteLine($"  {item.Extension,-10} : {item.Count}");
        total += item.Count;
      }
      Trace.WriteLine("");
      Trace.WriteLine($"  Total: {total}");
    }
#endregion
    static void ShowHelp()
    {
      Console.WriteLine("Usage: SPEAR [options]");
      Console.WriteLine();
      Console.WriteLine("Options:");
      Console.WriteLine("  /h, /help           Show this help message");
      Console.WriteLine("  /list               List count by file type");
      Console.WriteLine("  /listdupes          List duplicate email files (by size and fingerprint)");
      Console.WriteLine("  /deletedupes        Generate DeleteDuplicates.bat file");

      Console.WriteLine("  /removedupes        Remove duplicate records");
    }
    static string ConfigureLogging()
    {
      // Clear any existing listeners (important if called multiple times)
      Trace.Listeners.Clear();

      // Get base path from config
      string logDir = System.Configuration.ConfigurationManager.AppSettings.Get("LOGFILE_PATH");
      Directory.CreateDirectory(logDir); // ensure folder exists

      // Create filename with datetime
      string logFile = Path.Combine(logDir,
          $"Spear.Ingestion_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.log");

      // File listener
      var fileListener = new TextWriterTraceListener(logFile)
      {
        Name = "FileLogger",
        TraceOutputOptions = TraceOptions.DateTime | TraceOptions.ThreadId
      };

      // Console listener
      var consoleListener = new ConsoleTraceListener(useErrorStream: false)
      {
        TraceOutputOptions = TraceOptions.DateTime
      };

      // Register listeners
      Trace.Listeners.Add(fileListener);
      Trace.Listeners.Add(consoleListener);
      Trace.AutoFlush = true;
      return logFile;
    }
    static void Main(string[] args)
 {
      string listPath = "";
      string logFile = ConfigureLogging();

      Trace.WriteLine($"Started @ {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
      Trace.WriteLine("");

      // Load defaults from App.config
      FILES_PATH = System.Configuration.ConfigurationManager.AppSettings.Get("FILES_PATH");
      DELETEDUPES_FILE = System.Configuration.ConfigurationManager.AppSettings.Get("DELETEDUPES_FILE");

      // If no args or help requested
      if (args.Length == 0 || args.Any(a => a.Equals("/h", StringComparison.OrdinalIgnoreCase) ||
                                            a.Equals("/help", StringComparison.OrdinalIgnoreCase)))
      {
        ShowHelp();
        return;
      }

      // Walk through arguments
      for (int i = 0; i < args.Length; i++)
      {
        string arg = args[i].ToLower();

        switch (arg)
        {
          case "/list":
            // Default path from App.config
            listPath = FILES_PATH;

            // If next arg exists and isn’t another switch, treat it as override
            if (i + 1 < args.Length && !args[i + 1].StartsWith("/"))
            {
              listPath = args[i + 1];
              i++; // skip over the path
            }
            ListCountByFileType(listPath);
            break;

          case "/listdupes":
            listPath = FILES_PATH;
            if (i + 1 < args.Length && !args[i + 1].StartsWith("/"))
            {
              listPath = args[i + 1];
              i++; // skip over the path
            }
            ListDuplicates(listPath);
            break;

          case "/deletedupes":
            // Defaults
            string deletePath = FILES_PATH;
            string deleteCommandFile = DELETEDUPES_FILE;

            // First optional parameter (filepath)
            if (i + 1 < args.Length && !args[i + 1].StartsWith("/"))
            {
              deletePath = args[i + 1];
              i++;
            }

            // Second optional parameter (delete command file name)
            if (i + 1 < args.Length && !args[i + 1].StartsWith("/"))
            {
              deleteCommandFile = args[i + 1];
              i++;
            }

            GenerateDeleteDuplicatesBat(deletePath, deleteCommandFile);
            break;

          case "/removedupes":
            // RemoveDuplicates();
            break;

          default:
            Console.WriteLine($"Unknown option: {args[i]}");
            ShowHelp();
            break;
        }
      }
      Trace.WriteLine("");
      Trace.WriteLine($"Completed @ {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
      Trace.Flush();
      Trace.Close();
    }

    //static void Main(string[] args)
    //{

    //  // AppSettings

    //  CONN_STRING = System.Configuration.ConfigurationManager.AppSettings.Get("CONN_STRING");
    //  TEMP_PATH = System.Configuration.ConfigurationManager.AppSettings.Get("TEMP_PATH");
    //  ERROR_PATH = System.Configuration.ConfigurationManager.AppSettings.Get("ERROR_PATH");
    //  LOGFILE_PATH = System.Configuration.ConfigurationManager.AppSettings.Get("LOGFILE_PATH");
    //  EXTRACT = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings.Get("EXTRACT"));
    //  EXTRACT_PATH = System.Configuration.ConfigurationManager.AppSettings.Get("EXTRACT_PATH");
    //  SKIP_CALENDAR = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings.Get("SKIP_CALENDAR"));
    //  TRUNCATE_TABLES = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings.Get("TRUNCATE_TABLES"));

    //  // Trace listeners (for console and log file)

    //  Trace.Listeners.Clear();
    //  string logFile = LOGFILE_PATH + "EmailToSQL_" + String.Format("{0:yyyy-MM-dd_HH-mm-ss}", DateTime.Now) + ".log";
    //  TextWriterTraceListener twtl = new TextWriterTraceListener(logFile);
    //  twtl.Name = "TextLogger";
    //  twtl.TraceOutputOptions = TraceOptions.ThreadId | TraceOptions.DateTime;
    //  ConsoleTraceListener ctl = new ConsoleTraceListener(false);
    //  ctl.TraceOutputOptions = TraceOptions.DateTime;
    //  Trace.Listeners.Add(twtl);
    //  Trace.Listeners.Add(ctl);
    //  Trace.AutoFlush = true;

    //  // Start Time
    //  Trace.WriteLine("Started: " + DateTime.Now.ToLongTimeString());
    //  Trace.WriteLine("");

    //  ListCountByFileType();
    //  RemoveDuplicates();

    //  //if (EXTRACT)
    //  //{
    //  //  Extract();
    //  //}
    //  //else
    //  //{
    //  //  LoadEmails();
    //  //}

    //  // Stop time
    //  Trace.WriteLine("");
    //  Trace.WriteLine("Log file written to " + logFile);
    //  Trace.WriteLine("");
    //  Trace.WriteLine("Stopped: " + DateTime.Now.ToLongTimeString());
    //}
  }
}