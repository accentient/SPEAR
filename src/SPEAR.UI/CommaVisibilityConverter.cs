using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Collections.Generic;

namespace UI
{
    public class CommaVisibilityConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            var attachments = values[0] as IList<EmailAttachment>;
            var current = values[1] as EmailAttachment;
            if (attachments == null || current == null)
                return Visibility.Collapsed;
            return attachments.IndexOf(current) < attachments.Count - 1 ? Visibility.Visible : Visibility.Collapsed;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}