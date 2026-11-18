
using MessagePack;

namespace Common;

[MessagePackObject]
public class ServidorInfo
{
    [Key("name")]
    public string Name { get; set; } = "";

    [Key("rank")]
    public int Rank { get; set; }

    [Key("port")]
    public int Port { get; set; }
}
