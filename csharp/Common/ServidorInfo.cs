
using MessagePack;

namespace Common;

[MessagePackObject]
public class ServidorInfo
{
    [Key("name")]
    public string Name { get; set; } = "";

    [Key("rank")]
    public int Rank { get; set; }

    [Key("address")]
    public string Address { get; set; } = "";
}
