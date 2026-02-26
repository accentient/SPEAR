using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Windows;
using System.Windows.Input; // Add this for Cursor
using System.Windows.Media.Imaging; // Add this for BitmapImage
using System.Windows.Controls.Primitives; // Add this for DataGridColumnHeader
using System.Windows.Media; // Add this for VisualTreeHelper
using System.Windows.Controls; // <-- Add this line
using System.Linq; // Add this for LINQ
using System.Threading.Tasks; // Add this for Task

namespace UI
{
  /// <summary>
  /// Interaction logic for MainWindow.xaml
  /// </summary>
  public partial class MainWindow : Window
  {
    private List<Email> Emails = new List<Email>();

    public MainWindow()
    {
      InitializeComponent();
      SearchButton.Click += SearchButton_Click;
      Loaded += (s, e) => SearchTextBox.Focus(); // Set focus to search box on startup
      EmailDataGrid.PreviewKeyDown += EmailDataGrid_PreviewKeyDown; // Add this line
      EmailDataGrid.MouseDoubleClick += EmailDataGrid_MouseDoubleClick; // Add this line
      this.Icon = new BitmapImage(new Uri("pack://application:,,,/SPEAR.UI;component/spear.ico")); // Set window icon
    }

    private void SearchButton_Click(object sender, RoutedEventArgs e)
    {
      Mouse.OverrideCursor = Cursors.Wait; // Set hourglass
      try
      {
        LoadEmails(SearchTextBox.Text);

        // Reset grid columns to their starting widths
        // EmailID is hidden, so skip index 0
        EmailDataGrid.Columns[1].Width = 130;  // Date
        EmailDataGrid.Columns[2].Width = 300;  // Sender
        EmailDataGrid.Columns[3].Width = 300;  // To
        EmailDataGrid.Columns[4].Width = 225;  // Subject
        EmailDataGrid.Columns[5].Width = new DataGridLength(1, DataGridLengthUnitType.Star); // Body
        EmailDataGrid.Columns[6].Width = 40;   // AttachmentCount (paperclip)
      }
      finally
      {
        Mouse.OverrideCursor = null; // Reset cursor
      }
    }

    private void LoadEmails(string searchText)
    {
      string whereClause = $"WHERE CONTAINS((Sender, [To], Cc, Subject, Body), '{searchText}')";
      Emails.Clear();

      string query = @"SELECT TOP 3000 E.EmailID, E.SentReceived AS Date, E.Sender, E.[To], E.Subject, 
                        LEFT(E.Body, 100) AS BodySnippet,
                        ISNULL(EA.AttachmentCount, 0) AS AttachmentCount
                 FROM Email E
                 LEFT JOIN (SELECT EmailID, COUNT(*) AS AttachmentCount 
                           FROM EmailAttachment 
                           GROUP BY EmailID) EA ON E.EmailID = EA.EmailID
                 WHERE CONTAINS((E.Sender, E.[To], E.Cc, E.Subject, E.Body), @search)
                 ORDER BY E.SentReceived";
      string connectionString = "Server=Accentient;Database=Email;Trusted_Connection=True;TrustServerCertificate=True;";

      using (var connection = new SqlConnection(connectionString))
      using (var command = new SqlCommand(query, connection))
      {
        command.CommandTimeout = 120; // 2 minutes
        if (searchText.Contains(" "))
            searchText = $"\"{searchText}\"";
        command.Parameters.AddWithValue("@search", searchText);

        connection.Open();
        using (var reader = command.ExecuteReader())
        {
          while (reader.Read())
          {
            Emails.Add(new Email
            {
              EmailID = reader.GetInt32(0),
              Date = reader.GetDateTime(1),
              Sender = reader.GetString(2),
              To = reader.GetString(3),
              Subject = reader.GetString(4),
              BodySnippet = reader.GetString(5),
              AttachmentCount = reader.GetInt32(6)
            });
          }
        }
      }
      EmailDataGrid.ItemsSource = null;
      EmailDataGrid.ItemsSource = Emails;
      QueryLabel.Content = $"{whereClause} ... returned {Emails.Count} rows";
    }

    private void DeleteEmail(object sender, RoutedEventArgs e)
    {
      if (EmailDataGrid.SelectedItem is Email selectedEmail)
      {
        DeleteEmailsFromDatabase(new List<Email> { selectedEmail });
        LoadEmails(SearchTextBox.Text);
      }
    }

    private async void DeleteSelectedEmails()
    {
      var selectedEmails = EmailDataGrid.SelectedItems.Cast<Email>().ToList();
      if (selectedEmails.Count == 0) return;

      var result = MessageBox.Show(
          $"You are about to delete {selectedEmails.Count} emails.\nAre you sure?",
          "Confirm Delete",
          MessageBoxButton.YesNo,
          MessageBoxImage.Warning);

      if (result != MessageBoxResult.Yes)
          return;

      // Set cursor on UI thread
      Mouse.OverrideCursor = Cursors.Wait;
      try
      {
          // Run deletion and reload on background thread
          await Task.Run(() => DeleteEmailsFromDatabase(selectedEmails));
          Dispatcher.Invoke(() => LoadEmails(SearchTextBox.Text));
      }
      finally
      {
          // Reset cursor on UI thread
          Mouse.OverrideCursor = null;
      }
    }

    private void DeleteEmailsFromDatabase(IEnumerable<Email> emails)
    {
        string connectionString = "Server=Accentient;Database=Email;Trusted_Connection=True;TrustServerCertificate=True;";
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            foreach (var email in emails)
            {
                string deleteQuery = "DELETE FROM Email WHERE EmailID = @id";
                using (var command = new SqlCommand(deleteQuery, connection))
                {
                    command.Parameters.AddWithValue("@id", email.EmailID);
                    command.ExecuteNonQuery();
                }
            }
        }
    }

    private void EmailDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
      // Check if the double-click was on a column header
      DependencyObject originalSource = e.OriginalSource as DependencyObject;
      while (originalSource != null)
      {
        if (originalSource is DataGridColumnHeader)
        {
          // Double-click was on header, do nothing
          return;
        }
        originalSource = VisualTreeHelper.GetParent(originalSource);
      }

      if (EmailDataGrid.SelectedItem is Email selectedEmail)
      {
        var detailsWindow = new EmailDetailsWindow(selectedEmail) { Owner = this };
        detailsWindow.ShowDialog();
      }
    }

    private void EmailDataGrid_PreviewKeyDown(object sender, KeyEventArgs e)
    {
      if (e.Key == Key.Enter && EmailDataGrid.SelectedItem is Email emailToShow)
      {
        var detailsWindow = new EmailDetailsWindow(emailToShow) { Owner = this };
        detailsWindow.ShowDialog();
        e.Handled = true;
      }
      else if (e.Key == Key.Delete && EmailDataGrid.SelectedItems.Count > 0)
      {
        DeleteSelectedEmails();
        e.Handled = true;
      }
    }

    private void EmailListView_KeyDown(object sender, KeyEventArgs e)
    {
      if (e.Key == Key.Delete)
      {
        DeleteSelectedEmails();
      }
    }
  }
}