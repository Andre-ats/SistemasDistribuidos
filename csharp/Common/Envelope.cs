using MessagePack;

namespace Common;

[MessagePackObject]
public class Envelope
{
    [Key("type")]
    public string Type { get; set; } = "";

    [Key("ts")]
    public long Ts { get; set; }

    [Key("clock")]
    public long Clock { get; set; }

    [Key("from")]
    public string From { get; set; } = "";

    [Key("user")]
    public string? User { get; set; }

    [Key("channel")]
    public string? Channel { get; set; }

    [Key("text")]
    public string? Text { get; set; }

    [Key("status")]
    public string? Status { get; set; }

    [Key("error")]
    public string? Error { get; set; }

    [Key("channels")]
    public List<string>? Channels { get; set; }

    [Key("messages")]
    public List<Envelope>? Messages { get; set; }
    [Key("id")]
    public string? Id { get; set; }

    [Key("server")]
    public string? Server { get; set; }

    [Key("rank")]
    public int? Rank { get; set; }

    [Key("address")]
    public string? Address { get; set; }

    [Key("time")]
    public long? Time { get; set; }

    [Key("servers")]
    public List<ServidorInfo>? Servers { get; set; }

    [Key("logins")]
    public List<LoginRegistro>? Logins { get; set; }
}
