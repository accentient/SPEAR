using System;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media.Imaging;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Windows.Documents;

namespace UI
{
    public partial class EmailDetailsWindow : Window
    {
        public EmailDetailsWindow(Email email)
        {
            InitializeComponent();
            this.Icon = new BitmapImage(new Uri("pack://application:,,,/SPEAR.UI;component/spear.ico"));
            this.PreviewKeyDown += EmailDetailsWindow_PreviewKeyDown;

            var fullEmail = LoadFullEmail(email.EmailID);
            DataContext = fullEmail;
        }

        private Email LoadFullEmail(int emailId)
        {
            string connectionString = "Server=Accentient;Database=Email;Trusted_Connection=True;TrustServerCertificate=True;";
            string query = @"SELECT EmailID, SentReceived AS Date, Sender, [To], [BCC], Subject, Source, BodyType, Body, BodyHtml
                             FROM Email WHERE EmailID = @id";

            Email email = null;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@id", emailId);
                connection.Open();
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        email = new Email
                        {
                            EmailID = reader.GetInt32(0),
                            Date = reader.GetDateTime(1),
                            Sender = reader.GetString(2),
                            To = reader.GetString(3),
                            BCC = reader.IsDBNull(4) ? "" : reader.GetString(4),
                            Subject = reader.GetString(5),
                            Source = reader.IsDBNull(6) ? "" : reader.GetString(6),
                            BodyType = reader.IsDBNull(7) ? "" : reader.GetString(7),
                            Body = reader.IsDBNull(8) ? "" : reader.GetString(8),
                            BodyHtml = reader.IsDBNull(9) ? "" : reader.GetString(9)
                        };
                    }
                }
            }

            // Load attachments
            if (email != null)
            {
                email.Attachments = new List<EmailAttachment>();
                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand("SELECT Name FROM EmailAttachment WHERE EmailID = @id", connection))
                {
                    command.Parameters.AddWithValue("@id", emailId);
                    connection.Open();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            email.Attachments.Add(new EmailAttachment { Name = reader.GetString(0) });
                        }
                    }
                }
            }

            return email;
        }

        private void EmailDetailsWindow_PreviewKeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Escape)
            {
                this.Close();
            }
        }

        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void BodyBrowser_Loaded(object sender, RoutedEventArgs e)
        {
            if (DataContext is Email email && sender is System.Windows.Controls.WebBrowser browser)
            {
                browser.NavigateToString(email.BodyType?.Equals("Html", StringComparison.OrdinalIgnoreCase) == true ? email.BodyHtml ?? "" : email.Body ?? "");
            }
        }

        private void AttachmentHyperlink_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Hyperlink hyperlink && hyperlink.Tag is EmailAttachment attachment)
            {
                string ext = System.IO.Path.GetExtension(attachment.Name).ToLowerInvariant();
                bool isImage = AttachmentImageWindow.IsImageExtension(ext);
                bool isText = AttachmentImageWindow.IsTextExtension(ext);

                if (isImage || isText)
                {
                    // Fetch the attachment bytes from the database
                    byte[] fileBytes = null;
                    string connectionString = "Server=Accentient;Database=Email;Trusted_Connection=True;TrustServerCertificate=True;";
                    int emailId = 0;

                    // Try to get EmailID from DataContext (which is set to Email)
                    if (DataContext is Email email)
                    {
                        emailId = email.EmailID;
                    }

                    using (var connection = new SqlConnection(connectionString))
                    using (var command = new SqlCommand("SELECT Attachment FROM EmailAttachment WHERE Name = @name AND EmailID = @id", connection))
                    {
                        command.Parameters.AddWithValue("@name", attachment.Name);
                        command.Parameters.AddWithValue("@id", emailId);
                        connection.Open();
                        var result = command.ExecuteScalar();
                        if (result != DBNull.Value && result is byte[] bytes)
                        {
                            fileBytes = bytes;
                        }
                    }

                    if (fileBytes != null)
                    {
                        var attachmentWindow = new AttachmentImageWindow(attachment.Name, fileBytes);
                        attachmentWindow.Owner = this;
                        attachmentWindow.ShowDialog();
                    }
                    else
                    {
                        MessageBox.Show("Attachment data not found in the database for: " + attachment.Name, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
                else
                {
                    MessageBox.Show("Only image and text attachments can be previewed at this time.", "Attachment", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
        }
    }
}