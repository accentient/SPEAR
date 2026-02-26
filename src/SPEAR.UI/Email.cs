namespace UI
{
    public class EmailAttachment
    {
        public string Name { get; set; }
        // Optionally, add:
        // public byte[] Data { get; set; }
    }

    public class Email
    {
        public int EmailID { get; set; }
        public DateTime Date { get; set; }
        public string Sender { get; set; }
        public string To { get; set; }
        public string Subject { get; set; }
        public string BodySnippet { get; set; }
        public int AttachmentCount { get; set; }
        public string Source { get; set; }
        public string BCC { get; set; }
        public string BodyType { get; set; }
        public string BodyHtml { get; set; }
        public string Body { get; set; }
        public List<EmailAttachment> Attachments { get; set; } = new();
    }
}