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
}
