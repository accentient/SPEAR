using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace EmailToSQL
{
  internal class DBXMail
  {
    internal List<KeyValuePair<int, int>> Segments = new List<KeyValuePair<int, int>>();
  }

  internal class DBX : IDisposable
  {
    private const int DBX_MAGIC = -32330289; //4262637007;
    private List<DBXMail> mails = null;
    private FileStream stream = null;
    private BinaryReader reader = null;

    internal int Parse(string FileName)
    {
      int count = -1;
      CleanUp();
      mails = new List<DBXMail>();
      if (File.Exists(FileName))
      {
        stream = File.OpenRead(FileName);
        if (stream.Length > 512)
        {
          reader = new BinaryReader(stream);
          byte[] root = reader.ReadBytes(512);
          if (_dbx_int32(root, 0) == DBX_MAGIC)
          {
            if ((_dbx_int32(root, 228) == 0) == (_dbx_int32(root, 196) == 0))
            {
              if (_dbx_int32(root, 228) != 0 && _dbx_int32(root, 196) != 0)
              {
                _dbx_list_header(_dbx_int32(root, 228), 0, FileName);
                count = mails.Count;
              }
            }
          }
        }
      }
      return count;
    }

    private void _dbx_list_header(int Offset, int Parent, string FileName)
    {
      do
      {
        byte[] head;
        _dbx_read(Offset, out head, 24);
        if (_dbx_int32(head, 0) != Offset)
          throw new IndexOutOfRangeException(string.Format("Self {1} != Offset {2}", _dbx_int32(head, 0), Offset));
        if (_dbx_int32(head, 12) != Parent)
          throw new IndexOutOfRangeException(string.Format("Back {1} != Parent {2}", _dbx_int32(head, 12), Parent));
        if (_dbx_int32(head, 4) != 0)
          throw new IndexOutOfRangeException(string.Format("Zero {1} != 0", _dbx_int32(head, 4)));

        Offset += 24;
        int n = (_dbx_int32(head, 16) >> 8) & 0xff;
        for (int i = 0; i < n; i++)
        {
          byte[] list;
          _dbx_read(Offset, out list, 12);
          if (_dbx_int32(list, 0) != 0)
            _dbx_mail_header(_dbx_int32(list, 0));
          if (_dbx_int32(list, 4) != 0)
            _dbx_list_header(_dbx_int32(list, 4), _dbx_int32(head, 0), FileName);
          Offset += 12;
        }

        Parent = _dbx_int32(head, 0);
        Offset = _dbx_int32(head, 8);

      } while (Offset != 0);
    }

    private void _dbx_mail_header(int Offset)
    {
      int d_offset = 0;
      int s_offset = 0;
      int m_offset = 0;
      bool indirect = false;
      byte[] mail;
      _dbx_read(Offset, out mail, 12);
      if (_dbx_int32(mail, 0) != Offset)
        throw new IndexOutOfRangeException(string.Format("Self {1} != Offset {2}", _dbx_int32(mail, 0), Offset));
      Offset += 12;
      int n = (_dbx_int32(mail, 8) >> 16) & 0xff;
      if (n != 0)
      {
        for (int i = 0; i < n; i++)
        {
          byte[] info;
          _dbx_read(Offset, out info, 4);
          switch (info[0])
          {
            case 0x0e:
              s_offset = _dbx_int24(info, 1);
              break;
            case 0x12:
              d_offset = _dbx_int24(info, 1);
              break;
            case 0x04:
              m_offset = _dbx_int24(info, 1);
              indirect = true;
              break;
            case 0x84:
              m_offset = _dbx_int24(info, 1);
              break;
          }
          Offset += 4;
        }
      }
      if (m_offset != 0)
      {
        if (indirect)
        {
          byte[] offset;
          _dbx_read(Offset + m_offset, out offset, 4);
          m_offset = _dbx_int32(offset, 0);
        }
        _dbx_mail_message(m_offset);
      }
    }

    internal string Extract(int Message)
    {
      string result = string.Empty;
      if (Message >= 0 && Message < mails.Count)
      {
        using (MemoryStream writer = new MemoryStream())
        {
          byte[] buffer;
          for (int i = 0; i < mails[Message].Segments.Count; i++)
          {
            stream.Seek(mails[Message].Segments[i].Key, SeekOrigin.Begin);
            buffer = reader.ReadBytes(mails[Message].Segments[i].Value);
            writer.Write(buffer, 0, buffer.Length);
          }
          writer.Position = 0;
          buffer = new byte[writer.Length];
          writer.Read(buffer, 0, buffer.Length);
          writer.Close();
          result = Encoding.Default.GetString(buffer);
        }
      }
      return result;
    }

    internal void Extract(int Message, string FileName)
    {
      if (Message >= 0 && Message < mails.Count)
      {
        if (File.Exists(FileName))
          File.Delete(FileName);
        using (FileStream writer = File.Create(FileName))
        {
          for (int i = 0; i < mails[Message].Segments.Count; i++)
          {
            stream.Seek(mails[Message].Segments[i].Key, SeekOrigin.Begin);
            byte[] buffer = reader.ReadBytes(mails[Message].Segments[i].Value);
            writer.Write(buffer, 0, buffer.Length);
          }
        }
      }
    }

    private void _dbx_mail_message(int Offset)
    {
      int resid = 0;
      DBXMail mail = new DBXMail();
      int start = Offset;
      int end = 0;
      do
      {
        byte[] text;
        _dbx_read(Offset, out text, 16);
        Offset += 16;
        resid = _dbx_int32(text, 8);
        do
        {
          int n = Math.Min(resid, 4096);
          byte[] buffer;
          _dbx_read(Offset, out buffer, n); //mail content!
          mail.Segments.Add(new KeyValuePair<int, int>(Offset, buffer.Length));
          Offset += n;
          resid -= n;

        } while (resid > 0);
        int nextPos = _dbx_int32(text, 12);
        if (nextPos > end && nextPos != 0)
          end = Offset;
        Offset = nextPos;

      } while (Offset != 0);
      if (end > start)
        mails.Add(mail);
      else
      {
        // System.Diagnostics.Debug.WriteLine(string.Format("wrong {0} {1}", start, end));
        // System.Diagnostics.Trace.WriteLine(string.Format("  Error reading DBX {0} {1}", start, end));
      }
    }

    private void _dbx_read(int Offset, out byte[] Buffer, int Size)
    {
      if (reader.BaseStream.Seek(Offset, SeekOrigin.Begin) != Offset || (Buffer = reader.ReadBytes(Size)).Length != Size)
        throw new ArgumentException();
    }

    private int _dbx_int24(byte[] Buffer, int Offset)
    {
      return Buffer[Offset + 2] * 65536 +
          Buffer[Offset + 1] * 256 +
          Buffer[Offset];
    }

    private int _dbx_int32(byte[] Buffer, int Offset)
    {
      //return Buffer[Offset + 3] * 16777216 +
      //Buffer[Offset + 2] * 65536    +
      //Buffer[Offset + 1] * 256      +
      //Buffer[Offset];
      return BitConverter.ToInt32(Buffer, Offset);
    }

    private void CleanUp()
    {
      if (reader != null)
        reader.Close();
      if (stream != null)
        stream.Close();
    }

    void IDisposable.Dispose()
    {
      CleanUp();
    }
  }
}
