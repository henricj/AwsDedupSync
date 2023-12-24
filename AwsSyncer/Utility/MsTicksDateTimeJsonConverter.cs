// Copyright (c) 2016 Henric Jungheim <software@henric.org>
// 
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

using System;
using Newtonsoft.Json;

namespace AwsSyncer.Utility;

public class MsTicksDateTimeJsonConverter : JsonConverter
{
    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        var dateTime = (DateTime)value;

        var binary = dateTime.ToBinary();

        var bytes = BitConverter.GetBytes(binary);

        writer.WriteValue(bytes);
    }

    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        if (null == reader.Value)
            return null;

        if (reader.TokenType != JsonToken.Bytes)
        {
            if (reader.TokenType == JsonToken.Date)
                return (DateTime)reader.Value;

            return null;
        }

        var bytes = (byte[])reader.Value;

        var binary = BitConverter.ToInt64(bytes, 0);

        return DateTime.FromBinary(binary);
    }

    public override bool CanConvert(Type objectType) => typeof(DateTime) == objectType;
}
