
using MessagePack;

namespace Common;

[MessagePackObject]
public class LoginRegistro
{
    [Key("user")]
    public string User { get; set; } = "";

    [Key("ts")]
    public long Ts { get; set; }
}
