using System.Windows;
using System.Windows.Media.Imaging;
using System.IO;
using System.Windows.Input;
using System.Text;

namespace UI
{
    public partial class AttachmentImageWindow : Window
    {
        public AttachmentImageWindow(string fileName, byte[] fileBytes)
        {
            InitializeComponent();
            this.Title = fileName;

            this.PreviewKeyDown += AttachmentImageWindow_PreviewKeyDown;

            if (fileBytes == null)
            {
                MessageBox.Show("No attachment data to display.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                this.Close();
                return;
            }

            string ext = System.IO.Path.GetExtension(fileName).ToLowerInvariant();

            if (IsImageExtension(ext))
            {
                ShowImage(fileBytes);
            }
            else if (IsTextExtension(ext))
            {
                ShowText(fileBytes);
            }
            else
            {
                MessageBox.Show("Preview not supported for this file type.", "Attachment", MessageBoxButton.OK, MessageBoxImage.Information);
                this.Close();
            }
        }

        private void ShowImage(byte[] imageBytes)
        {
            using (var ms = new MemoryStream(imageBytes))
            {
                var bitmap = new BitmapImage();
                bitmap.BeginInit();
                bitmap.CacheOption = BitmapCacheOption.OnLoad;
                bitmap.StreamSource = ms;
                bitmap.EndInit();
                PreviewImage.Source = bitmap;
            }
            PreviewImage.Visibility = Visibility.Visible;
            TextScrollViewer.Visibility = Visibility.Collapsed;
        }

        private void ShowText(byte[] fileBytes)
        {
            string text;
            try
            {
                // Try UTF-8, fallback to default encoding if needed
                text = Encoding.UTF8.GetString(fileBytes);
            }
            catch
            {
                text = Encoding.Default.GetString(fileBytes);
            }
            PreviewText.Text = text;
            PreviewImage.Visibility = Visibility.Collapsed;
            TextScrollViewer.Visibility = Visibility.Visible;
        }

        public static bool IsImageExtension(string ext)
        {
            return ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".bmp" ||
                   ext == ".gif" || ext == ".tiff" || ext == ".tif" || ext == ".ico" ||
                   ext == ".wdp" || ext == ".jxr";
        }

        public static bool IsTextExtension(string ext)
        {
            return ext == ".txt" || ext == ".csv" || ext == ".sql" || ext == ".log" ||
                   ext == ".json" || ext == ".xml" || ext == ".ini" || ext == ".md" ||
                   ext == ".yaml" || ext == ".yml" || ext == ".bat" || ext == ".cmd" ||
                   ext == ".config";
        }

        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void AttachmentImageWindow_PreviewKeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Escape)
            {
                this.Close();
            }
        }
    }
}