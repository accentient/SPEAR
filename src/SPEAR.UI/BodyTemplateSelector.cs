using System.Windows;
using System.Windows.Controls;

namespace UI
{
    public class BodyTemplateSelector : DataTemplateSelector
    {
        public override DataTemplate SelectTemplate(object item, DependencyObject container)
        {
            var email = item as Email;
            // Get the window from the container
            var window = Window.GetWindow(container);
            if (email != null && window != null)
            {
                string key = email.BodyType?.Equals("Html", StringComparison.OrdinalIgnoreCase) == true
                    ? "HtmlTemplate"
                    : "PlainTextTemplate";

                if (window.Resources.Contains(key))
                    return window.FindResource(key) as DataTemplate;
            }
            return base.SelectTemplate(item, container);
        }
    }
}