namespace EKG.Common.Kafka.Examples;

using System.ComponentModel.DataAnnotations;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;

[DataContract]
public class MyDto
{
    [DataMember(Order = 0)] [Required] [Range(0, 125)]
    public int Age { get; set; }

    [DataMember(Order = 1)] [Required]
    public string FirstName { get; set; }

    [DataMember(Order = 2)]
    public string? LastName { get; set; }

    [IgnoreDataMember] [JsonIgnore]
    public string FullName => FirstName + LastName;
}

[DataContract]
public class MyAsyncDto
{
    [DataMember(Order = 0)] [Required] [Range(0, 125)]
    public int Age { get; set; }

    [DataMember(Order = 1)] [Required]
    public string FirstName { get; set; }

    [DataMember(Order = 2)]
    public string? LastName { get; set; }

    [IgnoreDataMember] [JsonIgnore]
    public string FullName => FirstName + LastName;
}

[DataContract] public class MyDtoProcessor : MyDto { }
[DataContract] public class MyDtoFiltered : MyDto { }
[DataContract] public class MyDtoProcessorBatching : MyDto { }
