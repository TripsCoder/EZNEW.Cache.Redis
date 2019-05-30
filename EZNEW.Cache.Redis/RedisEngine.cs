using EZNEW.Cache.Request;
using EZNEW.Cache.Response;
using EZNEW.Framework.ValueType;
using EZNEW.Framework.Extension;
using EZNEW.Framework.Serialize;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace EZNEW.Cache.Redis
{
    /// <summary>
    /// implements ICacheEngine by Redis
    /// </summary>
    public class RedisEngine : ICacheEngine
    {
        static ConcurrentDictionary<string, ConnectionMultiplexer> connectionDict = new ConcurrentDictionary<string, ConnectionMultiplexer>();

        #region string

        #region StringSetRange

        /// <summary>
        /// Overwrites part of the string stored at key, starting at the specified offset,
        /// for the entire length of value. If the offset is larger than the current length
        /// of the string at key, the string is padded with zero-bytes to make offset fit.
        /// Non-existing keys are considered as empty strings, so this command will make
        /// sure it holds a string large enough to be able to set value at offset.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string set range response</returns>
        public async Task<StringSetRangeResponse> StringSetRangeAsync(StringSetRangeRequest request)
        {
            var db = GetDB(request.Server);
            var newValue = await db.StringSetRangeAsync(request.Key, request.Offset, request.Value, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            long newLength = 0;
            long.TryParse(newValue, out newLength);
            return new StringSetRangeResponse()
            {
                Success = true,
                NewValueLength = newLength
            };
        }

        #endregion

        #region StringSetBit

        /// <summary>
        /// Sets or clears the bit at offset in the string value stored at key. The bit is
        /// either set or cleared depending on value, which can be either 0 or 1. When key
        /// does not exist, a new string value is created.The string is grown to make sure
        /// it can hold a bit at offset.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string set bit response</returns>
        public async Task<StringSetBitResponse> StringSetBitAsync(StringSetBitRequest request)
        {
            var db = GetDB(request.Server);
            var oldBitValue = await db.StringSetBitAsync(request.Key, request.Offset, request.Bit, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringSetBitResponse()
            {
                Success = true,
                OldBitValue = oldBitValue
            };
        }

        #endregion

        #region StringSet

        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten,
        /// regardless of its type.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string set response</returns>
        public async Task<StringSetResponse> StringSetAsync(StringSetRequest request)
        {
            var db = GetDB(request.Server);
            if (request.DataItems.IsNullOrEmpty())
            {
                return new StringSetResponse()
                {
                    Success = false,
                    Message = "not have any data items"
                };
            }
            List<StringItemSetResult> resultList = new List<StringItemSetResult>(request.DataItems.Count);
            foreach (var item in request.DataItems)
            {
                TimeSpan? expiry = null;
                if (item.Seconds > 0)
                {
                    expiry = TimeSpan.FromSeconds(item.Seconds);
                }
                var result = await db.StringSetAsync(item.Key, item.Value.ToString(), expiry: expiry, when: GetWhen(item.SetCondition), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                resultList.Add(new StringItemSetResult()
                {
                    Key = item.Key,
                    SetSuccess = result
                });
            }
            return new StringSetResponse()
            {
                Success = true,
                SetResults = resultList
            };
        }

        #endregion

        #region StringLength

        /// <summary>
        /// Returns the length of the string value stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string length response</returns>
        public async Task<StringLengthResponse> StringLengthAsync(StringLengthRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.StringLengthAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringLengthResponse()
            {
                Success = false,
                StringLength = length
            };
        }

        #endregion

        #region StringIncrement

        /// <summary>
        /// Increments the string representing a floating point number stored at key by the
        /// specified increment. If the key does not exist, it is set to 0 before performing
        /// the operation. The precision of the output is fixed at 17 digits after the decimal
        /// point regardless of the actual internal precision of the computation.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="request">request</param>
        /// <returns>string increment response</returns>
        public async Task<StringIncrementResponse<T>> StringIncrementAsync<T>(StringIncrementRequest<T> request)
        {
            var db = GetDB(request.Server);
            var typeCode = Type.GetTypeCode(typeof(T));
            T newValue = default(T);
            bool operation = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    long incrementValue = 0;
                    long.TryParse(request.Value.ToString(), out incrementValue);
                    var newLongValue = await db.StringIncrementAsync(request.Key, incrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newLongValue);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(request.Value.ToString(), out doubleIncrementValue);
                    var newDoubleValue = await db.StringIncrementAsync(request.Key, doubleIncrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newDoubleValue);
                    operation = true;
                    break;
            }
            return new StringIncrementResponse<T>()
            {
                Success = operation,
                NewValue = newValue
            };
        }

        #endregion

        #region StringGetWithExpiry

        /// <summary>
        /// Get the value of key. If the key does not exist the special value nil is returned.
        /// An error is returned if the value stored at key is not a string, because GET
        /// only handles string values.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string get with expiry response</returns>
        public async Task<StringGetWithExpiryResponse> StringGetWithExpiryAsync(StringGetWithExpiryRequest request)
        {
            var db = GetDB(request.Server);
            var valueWithExpiry = await db.StringGetWithExpiryAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringGetWithExpiryResponse()
            {
                Success = true,
                Value = valueWithExpiry.Value,
                Expiry = valueWithExpiry.Expiry
            };
        }

        #endregion

        #region StringGetSet

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string get set response</returns>
        public async Task<StringGetSetResponse> StringGetSetAsync(StringGetSetRequest request)
        {
            var db = GetDB(request.Server);
            var oldValue = await db.StringGetSetAsync(request.Key, request.NewValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringGetSetResponse()
            {
                Success = true,
                OldValue = oldValue
            };
        }

        #endregion

        #region StringGetRange

        /// <summary>
        /// Returns the substring of the string value stored at key, determined by the offsets
        /// start and end (both are inclusive). Negative offsets can be used in order to
        /// provide an offset starting from the end of the string. So -1 means the last character,
        /// -2 the penultimate and so forth.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string get range response</returns>
        public async Task<StringGetRangeResponse> StringGetRangeAsync(StringGetRangeRequest request)
        {
            var db = GetDB(request.Server);
            var rangeValue = await db.StringGetRangeAsync(request.Key, request.Start, request.End, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringGetRangeResponse()
            {
                Success = true,
                Value = rangeValue
            };
        }

        #endregion

        #region StringGetBit

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key. When offset
        /// is beyond the string length, the string is assumed to be a contiguous space with
        /// 0 bits
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string get bit response</returns>
        public async Task<StringGetBitResponse> StringGetBitAsync(StringGetBitRequest request)
        {
            var db = GetDB(request.Server);
            var bit = await db.StringGetBitAsync(request.Key, request.Offset, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringGetBitResponse()
            {
                Success = true,
                Bit = bit
            };
        }

        #endregion

        #region StringGet

        /// <summary>
        /// Returns the values of all specified keys. For every key that does not hold a
        /// string value or does not exist, the special value nil is returned.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string get response</returns>
        public async Task<StringGetResponse> StringGetAsync(StringGetRequest request)
        {
            if (request.Keys.IsNullOrEmpty())
            {
                return new StringGetResponse()
                {
                    Success = false,
                    Message = "keys is null or empty"
                };
            }
            var db = GetDB(request.Server);
            List<KeyItem> values = new List<KeyItem>(request.Keys.Count);
            foreach (var key in request.Keys)
            {
                var value = await db.StringGetAsync(key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                values.Add(new KeyItem()
                {
                    Key = key,
                    Value = value
                });
            }
            return new StringGetResponse()
            {
                Success = true,
                Values = values
            };
        }

        #endregion

        #region StringDecrement

        /// <summary>
        /// Decrements the number stored at key by decrement. If the key does not exist,
        /// it is set to 0 before performing the operation. An error is returned if the key
        /// contains a value of the wrong type or contains a string that is not representable
        /// as integer. This operation is limited to 64 bit signed integers.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="request">request</param>
        /// <returns>string decrement response</returns>
        public async Task<StringDecrementResponse<T>> StringDecrementAsync<T>(StringDecrementRequest<T> request)
        {
            var db = GetDB(request.Server);
            var typeCode = Type.GetTypeCode(typeof(T));
            T newValue = default(T);
            bool operation = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    long incrementValue = 0;
                    long.TryParse(request.Value.ToString(), out incrementValue);
                    var newLongValue = await db.StringDecrementAsync(request.Key, incrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newLongValue);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(request.Value.ToString(), out doubleIncrementValue);
                    var newDoubleValue = await db.StringDecrementAsync(request.Key, doubleIncrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newDoubleValue);
                    operation = true;
                    break;
            }
            return new StringDecrementResponse<T>()
            {
                Success = operation,
                NewValue = newValue
            };
        }

        #endregion

        #region StringBitPosition

        /// <summary>
        /// Return the position of the first bit set to 1 or 0 in a string. The position
        /// is returned thinking at the string as an array of bits from left to right where
        /// the first byte most significant bit is at position 0, the second byte most significant
        /// bit is at position 8 and so forth. An start and end may be specified; these are
        /// in bytes, not bits; start and end can contain negative values in order to index
        /// bytes starting from the end of the string, where -1 is the last byte, -2 is the
        /// penultimate, and so forth.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string bit position response</returns>
        public async Task<StringBitPositionResponse> StringBitPositionAsync(StringBitPositionRequest request)
        {
            var db = GetDB(request.Server);
            var position = await db.StringBitPositionAsync(request.Key, request.Bit, request.Start, request.End, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringBitPositionResponse()
            {
                Success = true,
                Position = position,
                HasValue = position >= 0
            };
        }

        #endregion

        #region StringBitOperation

        /// <summary>
        /// Perform a bitwise operation between multiple keys (containing string values)
        ///  and store the result in the destination key. The BITOP command supports four
        ///  bitwise operations; note that NOT is a unary operator: the second key should
        ///  be omitted in this case and only the first key will be considered. The result
        /// of the operation is always stored at destkey.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string bit operation response</returns>
        public async Task<StringBitOperationResponse> StringBitOperationAsync(StringBitOperationRequest request)
        {
            var db = GetDB(request.Server);
            var destionationValueLength = await db.StringBitOperationAsync(GetBitwise(request.Bitwise), request.DestinationKey, request.Keys.Select(c => { RedisKey key = c; return key; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringBitOperationResponse()
            {
                Success = true,
                DestinationValueLength = destionationValueLength
            };
        }

        #endregion

        #region StringBitCount

        /// <summary>
        /// Count the number of set bits (population counting) in a string. By default all
        /// the bytes contained in the string are examined.It is possible to specify the
        /// counting operation only in an interval passing the additional arguments start
        /// and end. Like for the GETRANGE command start and end can contain negative values
        /// in order to index bytes starting from the end of the string, where -1 is the
        /// last byte, -2 is the penultimate, and so forth.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string bit count response</returns>
        public async Task<StringBitCountResponse> StringBitCountAsync(StringBitCountRequest request)
        {
            var db = GetDB(request.Server);
            var count = await db.StringBitCountAsync(request.Key, request.Start, request.End, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new StringBitCountResponse()
            {
                Success = true,
                BitNum = count
            };
        }

        #endregion

        #region StringAppend

        /// <summary>
        /// If key already exists and is a string, this command appends the value at the
        /// end of the string. If key does not exist it is created and set as an empty string,
        /// so APPEND will be similar to SET in this special case.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>string append response</returns>
        public async Task<StringAppendResponse> StringAppendAsync(StringAppendRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.StringAppendAsync(request.Key, request.Value).ConfigureAwait(false);
            return new StringAppendResponse()
            {
                Success = false,
                NewValueLength = length
            };
        }

        #endregion

        #endregion

        #region list

        #region ListTrim

        /// <summary>
        /// Trim an existing list so that it will contain only the specified range of elements
        /// specified. Both start and stop are zero-based indexes, where 0 is the first element
        /// of the list (the head), 1 the next element and so on. For example: LTRIM foobar
        /// 0 2 will modify the list stored at foobar so that only the first three elements
        /// of the list will remain. start and end can also be negative numbers indicating
        /// offsets from the end of the list, where -1 is the last element of the list, -2
        /// the penultimate element and so on.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list trim response</returns>
        public async Task<ListTrimResponse> ListTrimAsync(ListTrimRequest request)
        {
            var db = GetDB(request.Server);
            await db.ListTrimAsync(request.Key, request.Start, request.Stop, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListTrimResponse()
            {
                Success = true
            };
        }

        #endregion

        #region ListSetByIndex

        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument,
        ///  see ListGetByIndex. An error is returned for out of range indexes.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list set by index response</returns>
        public async Task<ListSetByIndexResponse> ListSetByIndexAsync(ListSetByIndexRequest request)
        {
            var db = GetDB(request.Server);
            await db.ListSetByIndexAsync(request.Key, request.Index, request.Value, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListSetByIndexResponse()
            {
                Success = true
            };
        }

        #endregion

        #region ListRightPush

        /// <summary>
        /// Insert all the specified values at the tail of the list stored at key. If key
        /// does not exist, it is created as empty list before performing the push operation.
        /// Elements are inserted one after the other to the tail of the list, from the leftmost
        /// element to the rightmost element. So for instance the command RPUSH mylist a
        /// b c will result into a list containing a as first element, b as second element
        /// and c as third element.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list right push</returns>
        public async Task<ListRightPushResponse> ListRightPushAsync(ListRightPushRequest request)
        {
            var db = GetDB(request.Server);
            var newLength = await db.ListRightPushAsync(request.Key, request.Values.Select(c => { RedisValue value = c; return value; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListRightPushResponse()
            {
                Success = true,
                NewListLength = newLength
            };
        }

        #endregion

        #region ListRightPopLeftPush

        /// <summary>
        /// Atomically returns and removes the last element (tail) of the list stored at
        /// source, and pushes the element at the first element (head) of the list stored
        /// at destination.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list right pop left response</returns>
        public async Task<ListRightPopLeftPushResponse> ListRightPopLeftPushAsync(ListRightPopLeftPushRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.ListRightPopLeftPushAsync(request.SourceKey, request.DestinationKey, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListRightPopLeftPushResponse()
            {
                Success = true,
                PopValue = value
            };
        }

        #endregion

        #region ListRightPop

        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list right pop response</returns>
        public async Task<ListRightPopResponse> ListRightPopAsync(ListRightPopRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.ListRightPopAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListRightPopResponse()
            {
                Success = true,
                PopValue = value
            };
        }

        #endregion

        #region ListRemove

        /// <summary>
        /// Removes the first count occurrences of elements equal to value from the list
        /// stored at key. The count argument influences the operation in the following way
        /// count > 0: Remove elements equal to value moving from head to tail. count less 0:
        /// Remove elements equal to value moving from tail to head. count = 0: Remove all
        /// elements equal to value.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list remove response</returns>
        public async Task<ListRemoveResponse> ListRemoveAsync(ListRemoveRequest request)
        {
            var db = GetDB(request.Server);
            var removeCount = await db.ListRemoveAsync(request.Key, request.Value, request.Count, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListRemoveResponse()
            {
                Success = true,
                RemoveCount = removeCount
            };
        }

        #endregion

        #region ListRange

        /// <summary>
        /// Returns the specified elements of the list stored at key. The offsets start and
        /// stop are zero-based indexes, with 0 being the first element of the list (the
        /// head of the list), 1 being the next element and so on. These offsets can also
        /// be negative numbers indicating offsets starting at the end of the list.For example,
        /// -1 is the last element of the list, -2 the penultimate, and so on. Note that
        /// if you have a list of numbers from 0 to 100, LRANGE list 0 10 will return 11
        /// elements, that is, the rightmost item is included.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list range response</returns>
        public async Task<ListRangeResponse> ListRangeAsync(ListRangeRequest request)
        {
            var db = GetDB(request.Server);
            var values = await db.ListRangeAsync(request.Key, request.Start, request.Stop, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListRangeResponse()
            {
                Success = true,
                Values = values.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region ListLength

        /// <summary>
        /// Returns the length of the list stored at key. If key does not exist, it is interpreted
        ///  as an empty list and 0 is returned.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list length response</returns>
        public async Task<ListLengthResponse> ListLengthAsync(ListLengthRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.ListLengthAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListLengthResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #region ListLeftPush

        /// <summary>
        /// Insert the specified value at the head of the list stored at key. If key does
        ///  not exist, it is created as empty list before performing the push operations.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list left push response</returns>
        public async Task<ListLeftPushResponse> ListLeftPushAsync(ListLeftPushRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.ListLeftPushAsync(request.Key, request.Values.Select(c => { RedisValue rvalue = c; return rvalue; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListLeftPushResponse()
            {
                Success = true,
                NewListLength = length
            };
        }

        #endregion

        #region ListLeftPop

        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list left pop response</returns>
        public async Task<ListLeftPopResponse> ListLeftPopAsync(ListLeftPopRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.ListLeftPopAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListLeftPopResponse()
            {
                Success = true,
                PopValue = value
            };
        }

        #endregion

        #region ListInsertBefore

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list insert begore response</returns>
        public async Task<ListInsertBeforeResponse> ListInsertBeforeAsync(ListInsertBeforeRequest request)
        {
            var db = GetDB(request.Server);
            var newLength = await db.ListInsertBeforeAsync(request.Key, request.PivotValue, request.InsertValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListInsertBeforeResponse()
            {
                NewListLength = newLength,
                HasInsert = newLength > 0
            };
        }

        #endregion

        #region ListInsertAfter

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list insert after response</returns>
        public async Task<ListInsertAfterResponse> ListInsertAfterAsync(ListInsertAfterRequest request)
        {
            var db = GetDB(request.Server);
            var newLength = await db.ListInsertAfterAsync(request.Key, request.PivotValue, request.InsertValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListInsertAfterResponse()
            {
                Success = true,
                NewListLength = newLength,
                HasInsert = newLength > 0
            };
        }

        #endregion

        #region ListGetByIndex

        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based,
        /// so 0 means the first element, 1 the second element and so on. Negative indices
        /// can be used to designate elements starting at the tail of the list. Here, -1
        /// means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>list get by index response</returns>
        public async Task<ListGetByIndexResponse> ListGetByIndexAsync(ListGetByIndexRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.ListGetByIndexAsync(request.Key, request.Index, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new ListGetByIndexResponse()
            {
                Success = true,
                Value = value
            };
        }

        #endregion

        #endregion

        #region Hash

        #region HashValues

        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash values response</returns>
        public async Task<HashValuesResponse> HashValuesAsync(HashValuesRequest request)
        {
            var db = GetDB(request.Server);
            var hashValues = await db.HashValuesAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashValuesResponse()
            {
                Success = true,
                Values = hashValues.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region HashSet

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key
        ///  holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash set response</returns>
        public async Task<HashSetResponse> HashSetAsync(HashSetRequest request)
        {
            var db = GetDB(request.Server);
            await db.HashSetAsync(request.Key, request.HashValues.Select(c => new HashEntry(c.Key, c.Value)).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashSetResponse()
            {
                Success = true
            };
        }

        #endregion

        #region HashLength

        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash length response</returns>
        public async Task<HashLengthResponse> HashLengthAsync(HashLengthRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.HashLengthAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashLengthResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #region HashKeys

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash keys response</returns>
        public async Task<HashKeysResponse> HashKeysAsync(HashKeysRequest request)
        {
            var db = GetDB(request.Server);
            var keys = await db.HashKeysAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashKeysResponse()
            {
                Success = true,
                HashKeys = keys.Select(c => { string key = c; return key; }).ToList()
            };

        }

        #endregion

        #region HashIncrement

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment.
        /// If key does not exist, a new key holding a hash is created. If field does not
        /// exist or holds a string that cannot be interpreted as integer, the value is set
        /// to 0 before the operation is performed.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="request">request</param>
        /// <returns>hash increment response</returns>
        public async Task<HashIncrementResponse<T>> HashIncrementAsync<T>(HashIncrementRequest<T> request)
        {
            var db = GetDB(request.Server);
            var typeCode = Type.GetTypeCode(typeof(T));
            T newValue = default(T);
            bool operation = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    long incrementValue = 0;
                    long.TryParse(request.IncrementValue.ToString(), out incrementValue);
                    var newLongValue = await db.HashIncrementAsync(request.Key, request.HashField, incrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newLongValue);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(request.IncrementValue.ToString(), out doubleIncrementValue);
                    var newDoubleValue = await db.HashIncrementAsync(request.Key, request.HashField, doubleIncrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newDoubleValue);
                    operation = true;
                    break;
            }
            return new HashIncrementResponse<T>()
            {
                Success = operation,
                NewValue = newValue,
                Key = request.Key,
                HashField = request.HashField
            };
        }

        #endregion

        #region HashGet

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash get response</returns>
        public async Task<HashGetResponse> HashGetAsync(HashGetRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.HashGetAsync(request.Key, request.HashField, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashGetResponse()
            {
                Success = true,
                Value = value
            };
        }

        #endregion

        #region HashGetAll

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash get all response</returns>
        public async Task<HashGetAllResponse> HashGetAllAsync(HashGetAllRequest request)
        {
            var db = GetDB(request.Server);
            var hashValues = await db.HashGetAllAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashGetAllResponse()
            {
                Success = true,
                HashValues = hashValues.ToDictionary(c => { string key = c.Name; return key; }, c => { string value = c.Value; return value; })
            };
        }

        #endregion

        #region HashExists

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash exists response</returns>
        public async Task<HashExistsResponse> HashExistsAsync(HashExistsRequest request)
        {
            var db = GetDB(request.Server);
            var exist = await db.HashExistsAsync(request.Key, request.HashField, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashExistsResponse()
            {
                Success = true,
                ExistsField = exist
            };
        }

        #endregion

        #region HashDelete

        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields
        /// are ignored. Non-existing keys are treated as empty hashes and this command returns 0
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash delete response</returns>
        public async Task<HashDeleteResponse> HashDeleteAsync(HashDeleteRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.HashDeleteAsync(request.Key, request.HashField, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new HashDeleteResponse()
            {
                Success = true,
                DeleteResult = result
            };
        }

        #endregion

        #region HashDecrement

        /// <summary>
        /// Decrement the specified field of an hash stored at key, and representing a floating
        ///  point number, by the specified decrement. If the field does not exist, it is
        ///  set to 0 before performing the operation.
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="request">request</param>
        /// <returns>hash decrement response</returns>
        public async Task<HashDecrementResponse<T>> HashDecrementAsync<T>(HashDecrementRequest<T> request)
        {
            var db = GetDB(request.Server);
            var typeCode = Type.GetTypeCode(typeof(T));
            T newValue = default(T);
            bool operation = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    long incrementValue = 0;
                    long.TryParse(request.DecrementValue.ToString(), out incrementValue);
                    var newLongValue = await db.HashDecrementAsync(request.Key, request.HashField, incrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newLongValue);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(request.DecrementValue.ToString(), out doubleIncrementValue);
                    var newDoubleValue = await db.HashDecrementAsync(request.Key, request.HashField, doubleIncrementValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.ConvertToSimpleType<T>(newDoubleValue);
                    operation = true;
                    break;
            }
            return new HashDecrementResponse<T>()
            {
                Success = operation,
                NewValue = newValue,
                Key = request.Key,
                HashField = request.HashField
            };
        }

        #endregion

        #region HashScan

        /// <summary>
        /// The HSCAN command is used to incrementally iterate over a hash
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>hash scan response</returns>
        public async Task<HashScanResponse> HashScanAsync(HashScanRequest request)
        {
            var db = GetDB(request.Server);
            var values = await Task<List<HashEntry>>.Run(() =>
            {
                return db.HashScan(request.Key, request.Pattern, request.PageSize, GetCommandFlags(request.CommandFlags)).ToList();
            }).ConfigureAwait(false);
            return new HashScanResponse()
            {
                Success = true,
                HashValues = values.ToDictionary(c => { string key = c.Name; return key; }, c => { string value = c.Value; return value; })
            };
        }

        #endregion

        #endregion

        #region sets

        #region SetRemove

        /// <summary>
        /// Remove the specified member from the set stored at key. Specified members that
        /// are not a member of this set are ignored.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set remove response</returns>
        public async Task<SetRemoveResponse> SetRemoveAsync(SetRemoveRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.SetRemoveAsync(request.Key, request.RemoveValues.Select(c => { RedisValue value = c; return value; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetRemoveResponse()
            {
                Success = true,
                RemoveCount = result
            };
        }

        #endregion

        #region SetRandomMembers

        /// <summary>
        /// Return an array of count distinct elements if count is positive. If called with
        /// a negative count the behavior changes and the command is allowed to return the
        /// same element multiple times. In this case the numer of returned elements is the
        /// absolute value of the specified count.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set random members response</returns>
        public async Task<SetRandomMembersResponse> SetRandomMembersAsync(SetRandomMembersRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SetRandomMembersAsync(request.Key, request.Count, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetRandomMembersResponse()
            {
                Success = true,
                Members = members.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SetRandomMember

        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set random member</returns>
        public async Task<SetRandomMemberResponse> SetRandomMemberAsync(SetRandomMemberRequest request)
        {
            var db = GetDB(request.Server);
            var member = await db.SetRandomMemberAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetRandomMemberResponse()
            {
                Success = true,
                Member = member
            };
        }

        #endregion

        #region SetPop

        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set pop response</returns>
        public async Task<SetPopResponse> SetPopAsync(SetPopRequest request)
        {
            var db = GetDB(request.Server);
            var value = await db.SetPopAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetPopResponse()
            {
                Success = true,
                PopValue = value
            };
        }

        #endregion

        #region SetMove

        /// <summary>
        /// Move member from the set at source to the set at destination. This operation
        /// is atomic. In every given moment the element will appear to be a member of source
        /// or destination for other clients. When the specified element already exists in
        /// the destination set, it is only removed from the source set.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set move response</returns>
        public async Task<SetMoveResponse> SetMoveAsync(SetMoveRequest request)
        {
            var db = GetDB(request.Server);
            var moveResult = await db.SetMoveAsync(request.SourceKey, request.DestinationKey, request.MoveValue, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetMoveResponse()
            {
                Success = true,
                MoveResult = moveResult
            };
        }

        #endregion

        #region SetMembers

        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set members response</returns>
        public async Task<SetMembersResponse> SetMembersAsync(SetMembersRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SetMembersAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetMembersResponse()
            {
                Success = true,
                Members = members.Select(c => { string member = c; return member; }).ToList()
            };
        }

        #endregion

        #region SetLength

        /// <summary>
        /// Returns the set cardinality (number of elements) of the set stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set length response</returns>
        public async Task<SetLengthResponse> SetLengthAsync(SetLengthRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.SetLengthAsync(request.Key, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetLengthResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #region SetContains

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set contains response</returns>
        public async Task<SetContainsResponse> SetContainsAsync(SetContainsRequest request)
        {
            var db = GetDB(request.Server);
            var containsValue = await db.SetContainsAsync(request.Key, request.Value, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetContainsResponse()
            {
                Success = true,
                ContainsValue = containsValue
            };
        }

        #endregion

        #region SetCombine

        /// <summary>
        /// Returns the members of the set resulting from the specified operation against
        /// the given sets.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set combine response</returns>
        public async Task<SetCombineResponse> SetCombineAsync(SetCombineRequest request)
        {
            var db = GetDB(request.Server);
            var values = await db.SetCombineAsync(GetSetOperation(request.SetOperation), request.Keys.Select(c => { RedisKey key = c; return key; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetCombineResponse()
            {
                Success = true,
                CombineValues = values.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SetCombineAndStore

        /// <summary>
        /// This command is equal to SetCombine, but instead of returning the resulting set,
        ///  it is stored in destination. If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set combine and store response</returns>
        public async Task<SetCombineAndStoreResponse> SetCombineAndStoreAsync(SetCombineAndStoreRequest request)
        {
            var db = GetDB(request.Server);
            var newValueCount = await db.SetCombineAndStoreAsync(GetSetOperation(request.SetOperation), request.DestinationKey, request.SourceKeys.Select(c => { RedisKey key = c; return key; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetCombineAndStoreResponse()
            {
                Success = true,
                Count = newValueCount
            };
        }

        #endregion

        #region SetAdd

        /// <summary>
        /// Add the specified member to the set stored at key. Specified members that are
        /// already a member of this set are ignored. If key does not exist, a new set is
        /// created before adding the specified members.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>set add response</returns>
        public async Task<SetAddResponse> SetAddAsync(SetAddRequest request)
        {
            var db = GetDB(request.Server);
            var addResult = await db.SetAddAsync(request.Key, request.Value, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SetAddResponse()
            {
                Success = true,
                AddResult = addResult
            };
        }

        #endregion

        #endregion

        #region sorted set

        #region SortedSetScore

        /// <summary>
        /// Returns the score of member in the sorted set at key; If member does not exist
        /// in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set score response</returns>
        public async Task<SortedSetScoreResponse> SortedSetScoreAsync(SortedSetScoreRequest request)
        {
            var db = GetDB(request.Server);
            var score = await db.SortedSetScoreAsync(request.Key, request.Member, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetScoreResponse()
            {
                Success = true,
                Score = score
            };
        }

        #endregion

        #region SortedSetRemoveRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this command removes all elements in the sorted
        /// set stored at key between the lexicographical range specified by min and max.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set remove range by value response</returns>
        public async Task<SortedSetRemoveRangeByValueResponse> SortedSetRemoveRangeByValueAsync(SortedSetRemoveRangeByValueRequest request)
        {
            var db = GetDB(request.Server);
            var removeCount = await db.SortedSetRemoveRangeByValueAsync(request.Key, request.MinValue, request.MaxValue, exclude: GetExclude(request.Exclude), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByValueResponse()
            {
                RemoveCount = removeCount,
                Success = true
            };
        }

        #endregion

        #region SortedSetRemoveRangeByScore

        /// <summary>
        /// Removes all elements in the sorted set stored at key with a score between min
        ///  and max (inclusive by default).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set remove range by score response</returns>
        public async Task<SortedSetRemoveRangeByScoreResponse> SortedSetRemoveRangeByScoreAsync(SortedSetRemoveRangeByScoreRequest request)
        {
            var db = GetDB(request.Server);
            var removeCount = await db.SortedSetRemoveRangeByScoreAsync(request.Key, request.Start, request.Stop, exclude: GetExclude(request.Exclude), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByScoreResponse()
            {
                RemoveCount = removeCount,
                Success = true
            };
        }

        #endregion

        #region SortedSetRemoveRangeByRank

        /// <summary>
        /// Removes all elements in the sorted set stored at key with rank between start
        /// and stop. Both start and stop are 0 -based indexes with 0 being the element with
        /// the lowest score. These indexes can be negative numbers, where they indicate
        /// offsets starting at the element with the highest score. For example: -1 is the
        /// element with the highest score, -2 the element with the second highest score
        /// and so forth.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set remove range by rank response</returns>
        public async Task<SortedSetRemoveRangeByRankResponse> SortedSetRemoveRangeByRankAsync(SortedSetRemoveRangeByRankRequest request)
        {
            var db = GetDB(request.Server);
            var removeCount = await db.SortedSetRemoveRangeByRankAsync(request.Key, request.Start, request.Stop, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByRankResponse()
            {
                RemoveCount = removeCount,
                Success = true
            };
        }

        #endregion

        #region SortedSetRemove

        /// <summary>
        /// Removes the specified members from the sorted set stored at key. Non existing
        /// members are ignored.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set remove response</returns>
        public async Task<SortedSetRemoveResponse> SortedSetRemoveAsync(SortedSetRemoveRequest request)
        {
            var db = GetDB(request.Server);
            var removeCount = await db.SortedSetRemoveAsync(request.Key, request.RemoveMembers.Select(c => { RedisValue value = c; return value; }).ToArray(), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveResponse()
            {
                Success = true,
                RemoveCount = removeCount
            };
        }

        #endregion

        #region SortedSetRank

        /// <summary>
        /// Returns the rank of member in the sorted set stored at key, by default with the
        /// scores ordered from low to high. The rank (or index) is 0-based, which means
        /// that the member with the lowest score has rank 0.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set rank response</returns>
        public async Task<SortedSetRankResponse> SortedSetRankAsync(SortedSetRankRequest request)
        {
            var db = GetDB(request.Server);
            var rank = await db.SortedSetRankAsync(request.Key, request.Member, GetSortedOrder(request.Order), flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRankResponse()
            {
                Success = true,
                Rank = rank
            };
        }

        #endregion

        #region SortedSetRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this command returns all the elements in the
        /// sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set range by value response</returns>
        public async Task<SortedSetRangeByValueResponse> SortedSetRangeByValueAsync(SortedSetRangeByValueRequest request)
        {
            var db = GetDB(request.Server);
            RedisValue minValue = default(RedisValue);
            if (!request.MinValue.IsNullOrEmpty())
            {
                minValue = request.MinValue;
            }
            RedisValue maxValue = default(RedisValue);
            if (!request.MaxValue.IsNullOrEmpty())
            {
                maxValue = request.MaxValue;
            }
            var values = await db.SortedSetRangeByValueAsync(request.Key, min: minValue, max: maxValue, exclude: GetExclude(request.Exclude), skip: request.Skip, take: request.Take, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByValueResponse()
            {
                Success = true,
                Members = values.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SortedSetRangeByScoreWithScores

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Start and stop are
        /// used to specify the min and max range for score values. Similar to other range
        /// methods the values are inclusive.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set range by score with scores response</returns>
        public async Task<SortedSetRangeByScoreWithScoresResponse> SortedSetRangeByScoreWithScoresAsync(SortedSetRangeByScoreWithScoresRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SortedSetRangeByScoreWithScoresAsync(request.Key, request.Start, request.Stop, exclude: GetExclude(request.Exclude), order: GetSortedOrder(request.Order), skip: request.Skip, take: request.Take, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByScoreWithScoresResponse()
            {
                Success = true,
                Members = members.Select(c => new SortedSetValueItem() { Value = c.Element, Score = c.Score }).ToList()
            };
        }

        #endregion

        #region SortedSetRangeByScore

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Start and stop are
        /// used to specify the min and max range for score values. Similar to other range
        /// methods the values are inclusive.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set range by score response</returns>
        public async Task<SortedSetRangeByScoreResponse> SortedSetRangeByScoreAsync(SortedSetRangeByScoreRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SortedSetRangeByScoreAsync(request.Key, request.Start, request.Stop, exclude: GetExclude(request.Exclude), order: GetSortedOrder(request.Order), skip: request.Skip, take: request.Take, flags: GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByScoreResponse()
            {
                Success = true,
                Members = members.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SortedSetRangeByRankWithScores

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Both start and stop
        /// are zero-based indexes, where 0 is the first element, 1 is the next element and
        /// so on. They can also be negative numbers indicating offsets from the end of the
        /// sorted set, with -1 being the last element of the sorted set, -2 the penultimate
        /// element and so on.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set range by rank with scores response</returns>
        public async Task<SortedSetRangeByRankWithScoresResponse> SortedSetRangeByRankWithScoresAsync(SortedSetRangeByRankWithScoresRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SortedSetRangeByRankWithScoresAsync(request.Key, request.Start, request.Stop, GetSortedOrder(request.Order), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByRankWithScoresResponse()
            {
                Success = true,
                Members = members.Select(c => new SortedSetValueItem() { Value = c.Element, Score = c.Score }).ToList()
            };
        }

        #endregion

        #region SortedSetRangeByRank

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Both start and stop
        /// are zero-based indexes, where 0 is the first element, 1 is the next element and
        /// so on. They can also be negative numbers indicating offsets from the end of the
        /// sorted set, with -1 being the last element of the sorted set, -2 the penultimate
        /// element and so on.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set range by rank response</returns>
        public async Task<SortedSetRangeByRankResponse> SortedSetRangeByRankAsync(SortedSetRangeByRankRequest request)
        {
            var db = GetDB(request.Server);
            var members = await db.SortedSetRangeByRankAsync(request.Key, request.Start, request.Stop, GetSortedOrder(request.Order), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByRankResponse()
            {
                Success = true,
                Members = members.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SortedSetLengthByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this command returns the number of elements
        /// in the sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="request">response</param>
        /// <returns>sorted set lenght by value response</returns>
        public async Task<SortedSetLengthByValueResponse> SortedSetLengthByValueAsync(SortedSetLengthByValueRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.SortedSetLengthByValueAsync(request.Key, request.MinValue, request.MaxValue, GetExclude(request.Exclude), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetLengthByValueResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #region SortedSetLength

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set stored
        /// at key.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set length response</returns>
        public async Task<SortedSetLengthResponse> SortedSetLengthAsync(SortedSetLengthRequest request)
        {
            var db = GetDB(request.Server);
            var length = await db.SortedSetLengthAsync(request.Key, request.Min, request.Max, GetExclude(request.Exclude), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetLengthResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #region SortedSetIncrement

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set increment response</returns>
        public async Task<SortedSetIncrementResponse> SortedSetIncrementAsync(SortedSetIncrementRequest request)
        {
            var db = GetDB(request.Server);
            var newScore = await db.SortedSetIncrementAsync(request.Key, request.Member, request.IncrementScore, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetIncrementResponse()
            {
                Success = true,
                NewScore = newScore
            };
        }

        #endregion

        #region SortedSetDecrement

        /// <summary>
        /// Decrements the score of member in the sorted set stored at key by decrement.
        /// If member does not exist in the sorted set, it is added with -decrement as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set decrement response</returns>
        public async Task<SortedSetDecrementResponse> SortedSetDecrementAsync(SortedSetDecrementRequest request)
        {
            var db = GetDB(request.Server);
            var newScore = await db.SortedSetDecrementAsync(request.Key, request.Member, request.DecrementScore, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetDecrementResponse()
            {
                Success = true,
                NewScore = newScore
            };
        }

        #endregion

        #region SortedSetCombineAndStore

        /// <summary>
        /// Computes a set operation over multiple sorted sets (optionally using per-set
        /// weights), and stores the result in destination, optionally performing a specific
        /// aggregation (defaults to sum)
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set combine and store response</returns>
        public async Task<SortedSetCombineAndStoreResponse> SortedSetCombineAndStoreAsync(SortedSetCombineAndStoreRequest request)
        {
            var db = GetDB(request.Server);
            var newSetLength = await db.SortedSetCombineAndStoreAsync(GetSetOperation(request.SetOperation), request.DestinationKey, request.SourceKeys.Select(c => { RedisKey key = c; return key; }).ToArray(), request.Weights, GetAggregate(request.Aggregate), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetCombineAndStoreResponse()
            {
                Success = true,
                NewSetLength = newSetLength
            };
        }

        #endregion

        #region SortedSetAdd

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored
        /// at key. If a specified member is already a member of the sorted set, the score
        /// is updated and the element reinserted at the right position to ensure the correct
        /// ordering.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sorted set add response</returns>
        public async Task<SortedSetAddResponse> SortedSetAddAsync(SortedSetAddRequest request)
        {
            var db = GetDB(request.Server);
            var newLength = await db.SortedSetAddAsync(request.Key, request.Members.Select(c => new SortedSetEntry(c.Value, c.Score)).ToArray(), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortedSetAddResponse()
            {
                Success = true,
                Length = newLength
            };
        }

        #endregion

        #endregion

        #region sort

        #region Sort

        /// <summary>
        /// Sorts a list, set or sorted set (numerically or alphabetically, ascending by
        /// default){await Task.Delay(100);return null;} By default, the elements themselves are compared, but the values can
        /// also be used to perform external key-lookups using the by parameter. By default,
        /// the elements themselves are returned, but external key-lookups (one or many)
        /// can be performed instead by specifying the get parameter (note that # specifies
        /// the element itself, when used in get). Referring to the redis SORT documentation
        /// for examples is recommended. When used in hashes, by and get can be used to specify
        /// fields using -> notation (again, refer to redis documentation).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sort response</returns>
        public async Task<SortResponse> SortAsync(SortRequest request)
        {
            var db = GetDB(request.Server);
            RedisValue byValue = default(RedisValue);
            if (!request.By.IsNullOrEmpty())
            {
                byValue = request.By;
            }
            RedisValue[] getValues = null;
            if (!request.Gets.IsNullOrEmpty())
            {
                getValues = request.Gets.Select(c => { RedisValue value = c; return value; }).ToArray();
            }
            var values = await db.SortAsync(request.Key, request.Skip, request.Take, GetSortedOrder(request.Order), GetSortType(request.SortType), byValue, getValues, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortResponse()
            {
                Success = true,
                Values = values.Select(c => { string value = c; return value; }).ToList()
            };
        }

        #endregion

        #region SortAndStore

        /// <summary>
        /// Sorts a list, set or sorted set (numerically or alphabetically, ascending by
        /// default){await Task.Delay(100);return null;} By default, the elements themselves are compared, but the values can
        /// also be used to perform external key-lookups using the by parameter. By default,
        /// the elements themselves are returned, but external key-lookups (one or many)
        /// can be performed instead by specifying the get parameter (note that # specifies
        /// the element itself, when used in get). Referring to the redis SORT documentation
        /// for examples is recommended. When used in hashes, by and get can be used to specify
        /// fields using -> notation (again, refer to redis documentation).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>sort and store response</returns>
        public async Task<SortAndStoreResponse> SortAndStoreAsync(SortAndStoreRequest request)
        {
            var db = GetDB(request.Server);
            RedisValue byValue = default(RedisValue);
            if (!request.By.IsNullOrEmpty())
            {
                byValue = request.By;
            }
            RedisValue[] getValues = null;
            if (!request.Gets.IsNullOrEmpty())
            {
                getValues = request.Gets.Select(c => { RedisValue value = c; return value; }).ToArray();
            }
            var length = await db.SortAndStoreAsync(request.DestinationKey, request.SourceKey, request.Skip, request.Take, GetSortedOrder(request.Order), GetSortType(request.SortType), byValue, getValues, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new SortAndStoreResponse()
            {
                Success = true,
                Length = length
            };
        }

        #endregion

        #endregion

        #region key

        #region KeyType

        /// <summary>
        /// Returns the string representation of the type of the value stored at key. The
        /// different types that can be returned are: string, list, set, zset and hash.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key type response</returns>
        public async Task<KeyTypeResponse> KeyTypeAsync(KeyTypeRequest request)
        {
            var db = GetDB(request.Server);
            var keyType = await db.KeyTypeAsync(request.Key, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyTypeResponse()
            {
                Success = true,
                KeyType = GetCacheKeyType(keyType)
            };
        }

        #endregion

        #region KeyTimeToLive

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout. This introspection
        /// capability allows a Redis client to check how many seconds a given key will continue
        /// to be part of the dataset.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key time to live response</returns>
        public async Task<KeyTimeToLiveResponse> KeyTimeToLiveAsync(KeyTimeToLiveRequest request)
        {
            var db = GetDB(request.Server);
            var timeSpan = await db.KeyTimeToLiveAsync(request.Key, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyTimeToLiveResponse()
            {
                Success = true,
                TimeToLive = timeSpan
            };
        }

        #endregion

        #region KeyRestore

        /// <summary>
        /// Create a key associated with a value that is obtained by deserializing the provided
        /// serialized value (obtained via DUMP). If ttl is 0 the key is created without
        /// any expire, otherwise the specified expire time(in milliseconds) is set.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key restore response</returns>
        public async Task<KeyRestoreResponse> KeyRestoreAsync(KeyRestoreRequest request)
        {
            var db = GetDB(request.Server);
            await db.KeyRestoreAsync(request.Key, request.Value, request.Expiry, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyRestoreResponse()
            {
                Success = true
            };
        }

        #endregion

        #region KeyRename

        /// <summary>
        /// Renames key to newkey. It returns an error when the source and destination names
        /// are the same, or when key does not exist.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key rename response</returns>
        public async Task<KeyRenameResponse> KeyRenameAsync(KeyRenameRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.KeyRenameAsync(request.Key, request.NewKey, GetWhen(request.When), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyRenameResponse()
            {
                Success = true,
                RenameResult = result
            };
        }

        #endregion

        #region KeyRandom

        /// <summary>
        /// Return a random key from the currently selected database.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key random response</returns>
        public async Task<KeyRandomResponse> KeyRandomAsync(KeyRandomRequest request)
        {
            var db = GetDB(request.Server);
            var key = await db.KeyRandomAsync(GetCommandFlags(request.CommandFlags));
            return new KeyRandomResponse()
            {
                Success = true,
                Key = key
            };
        }

        #endregion

        #region KeyPersist

        /// <summary>
        /// Remove the existing timeout on key, turning the key from volatile (a key with
        /// an expire set) to persistent (a key that will never expire as no timeout is associated).
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key persist response</returns>
        public async Task<KeyPersistResponse> KeyPersistAsync(KeyPersistRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.KeyPersistAsync(request.Key, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyPersistResponse()
            {
                Success = true,
                OperationResult = result
            };
        }

        #endregion

        #region KeyMove

        /// <summary>
        /// Move key from the currently selected database (see SELECT) to the specified destination
        /// database. When key already exists in the destination database, or it does not
        /// exist in the source database, it does nothing. It is possible to use MOVE as
        /// a locking primitive because of this.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key move response</returns>
        public async Task<KeyMoveResponse> KeyMoveAsync(KeyMoveRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.KeyMoveAsync(request.Key, request.DataBase, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyMoveResponse()
            {
                Success = true,
                OperationResult = result
            };
        }

        #endregion

        #region KeyMigrate

        /// <summary>
        /// Atomically transfer a key from a source Redis instance to a destination Redis
        /// instance. On success the key is deleted from the original instance by default,
        /// and is guaranteed to exist in the target instance.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key migrate response</returns>
        public async Task<KeyMigrateResponse> KeyMigrateAsync(KeyMigrateRequest request)
        {
            var db = GetDB(request.Server);
            await db.KeyMigrateAsync(request.Key, new IPEndPoint(new IPAddress(Encoding.UTF8.GetBytes(request.Server?.Host)), request.Server?.Port ?? 0), int.Parse(request.Server?.Db ?? "0"), request.TimeOutMilliseconds, GetMigrateOptions(request.MigrateOptions), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyMigrateResponse()
            {
                Success = true
            };
        }

        #endregion

        #region KeyExpire

        /// <summary>
        /// Set a timeout on key. After the timeout has expired, the key will automatically
        /// be deleted. A key with an associated timeout is said to be volatile in Redis
        /// terminology.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key expire response</returns>
        public async Task<KeyExpireResponse> KeyExpireAsync(KeyExpireRequest request)
        {
            var db = GetDB(request.Server);
            var result = await db.KeyExpireAsync(request.Key, request.Expire, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyExpireResponse()
            {
                Success = true,
                OperationResult = result
            };
        }

        #endregion;

        #region KeyDump

        /// <summary>
        /// Serialize the value stored at key in a Redis-specific format and return it to
        /// the user. The returned value can be synthesized back into a Redis key using the
        /// RESTORE command.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key dump response</returns>
        public async Task<KeyDumpResponse> KeyDumpAsync(KeyDumpRequest request)
        {
            var db = GetDB(request.Server);
            var byteValues = await db.KeyDumpAsync(request.Key, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyDumpResponse()
            {
                Success = true,
                ByteValues = byteValues
            };
        }

        #endregion

        #region KeyDelete

        /// <summary>
        /// Removes the specified keys. A key is ignored if it does not exist.
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>key delete response</returns>
        public async Task<KeyDeleteResponse> KeyDeleteAsync(KeyDeleteRequest request)
        {
            var db = GetDB(request.Server);
            var count = await db.KeyDeleteAsync(request.Keys.Select(c => { RedisKey key = c; return key; }).ToArray(), GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            return new KeyDeleteResponse()
            {
                Success = true,
                DeleteCount = count
            };
        }

        #endregion

        #endregion

        #region server command

        #region get all data base

        /// <summary>
        /// get all database
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>get all database response</returns>
        public async Task<GetAllDataBaseResponse> GetAllDataBaseAsync(GetAllDataBaseRequest request)
        {
            var dataBaseList = await Task<List<CacheDb>>.Run(() =>
            {
                if (request.Server == null)
                {
                    return new List<CacheDb>(0);
                }
                var conn = GetConnection(request.Server);
                var configs = conn.GetServer(string.Format("{0}:{1}", request.Server, request.Server)).ConfigGet("databases");
                if (configs == null || configs.Length <= 0)
                {
                    return new List<CacheDb>(0);
                }
                var dataBaseConfig = configs.FirstOrDefault(c => c.Key.ToLower() == "databases");
                int dataBaseSize = dataBaseConfig.Value.ObjToInt32();
                List<CacheDb> dbList = new List<CacheDb>();
                for (var d = 0; d < dataBaseSize; d++)
                {
                    dbList.Add(new CacheDb()
                    {
                        Index = d,
                        Name = string.Format("数据库{0}", d)
                    });
                }
                return dbList;
            }).ConfigureAwait(false);
            return new GetAllDataBaseResponse()
            {
                Success = true,
                DataBaseList = dataBaseList
            };
        }

        #endregion

        #region query keys

        /// <summary>
        /// query keys
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>get keys response</returns>
        public async Task<GetKeysResponse> GetKeysAsync(GetKeysRequest request)
        {
            var keyItemPaging = await Task<CachePaging<KeyItem>>.Run(async () =>
             {
                 var server = request.Server;
                 var query = request.Query;
                 if (server == null)
                 {
                     return CachePaging<KeyItem>.EmptyPaging();
                 }
                 var db = GetDB(server);
                 string searchString = "*";
                 if (query != null && !string.IsNullOrWhiteSpace(query.MateKey))
                 {
                     searchString = string.Format("*{0}*", query.MateKey);
                 }
                 var redisServer = db.Multiplexer.GetServer(string.Format("{0}:{1}", server.Host, server.Port));//(query.Page - 1) * query.PageSize
                 var keys = redisServer.Keys(db.Database, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
                 List<KeyItem> itemList = new List<KeyItem>();
                 foreach (var key in keys)
                 {
                     KeyItem item = new KeyItem();
                     item.Key = key;
                     var redisType = await db.KeyTypeAsync(key, CommandFlags.None).ConfigureAwait(false);
                     item.Type = GetCacheKeyType(redisType);
                     itemList.Add(item);
                 }
                 var totalCount = redisServer.DatabaseSize(db.Database);
                 return new CachePaging<KeyItem>(query.Page, query.PageSize, totalCount, itemList);
             }).ConfigureAwait(false);
            return new GetKeysResponse()
            {
                Success = true,
                Keys = keyItemPaging
            };
        }

        #endregion

        #region clear data

        /// <summary>
        /// clear database data
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>clear data response</returns>
        public async Task<ClearDataResponse> ClearDataAsync(ClearDataRequest request)
        {
            var server = request.Server;
            if (request.DataBaseList.IsNullOrEmpty())
            {
                return new ClearDataResponse()
                {
                    Success = false,
                    Message = "databaselist is null or empty"
                };
            }
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
            foreach (var db in request.DataBaseList)
            {
                await redisServer.FlushDatabaseAsync(db?.Index ?? 0, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            }
            return new ClearDataResponse()
            {
                Success = true
            };
        }

        #endregion

        #region get cache item detail

        /// <summary>
        /// get cache item detail
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>get key detail response</returns>
        public async Task<GetKeyDetailResponse> GetKeyDetailAsync(GetKeyDetailRequest request)
        {
            var server = request.Server;
            var db = GetDB(server);
            var redisType = await db.KeyTypeAsync(request.Key, GetCommandFlags(request.CommandFlags)).ConfigureAwait(false);
            KeyItem keyItem = new KeyItem()
            {
                Key = request.Key,
                Type = GetCacheKeyType(redisType)
            };
            switch (redisType)
            {
                case RedisType.String:
                    keyItem.Value = await db.StringGetAsync(keyItem.Key).ConfigureAwait(false);
                    break;
                case RedisType.List:
                    List<string> listValues = new List<string>();
                    var listResults = await db.ListRangeAsync(keyItem.Key, 0, -1, CommandFlags.None).ConfigureAwait(false);
                    listValues.AddRange(listResults.Select(c => (string)c));
                    keyItem.Value = listValues;
                    break;
                case RedisType.Set:
                    List<string> setValues = new List<string>();
                    var setResults = await db.SetMembersAsync(keyItem.Key, CommandFlags.None).ConfigureAwait(false);
                    setValues.AddRange(setResults.Select(c => (string)c));
                    keyItem.Value = setValues;
                    break;
                case RedisType.SortedSet:
                    List<string> sortSetValues = new List<string>();
                    var sortedResults = await db.SortedSetRangeByRankAsync(keyItem.Key).ConfigureAwait(false);
                    sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                    keyItem.Value = sortSetValues;
                    break;
                case RedisType.Hash:
                    Dictionary<string, string> hashValues = new Dictionary<string, string>();
                    var objValues = await db.HashGetAllAsync(keyItem.Key).ConfigureAwait(false);
                    foreach (var obj in objValues)
                    {
                        hashValues.Add(obj.Name, obj.Value);
                    }
                    keyItem.Value = hashValues;
                    break;
            }
            return new GetKeyDetailResponse()
            {
                Success = true,
                KeyDetail = keyItem
            };
        }

        #endregion

        #region get server config

        /// <summary>
        /// get server config
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>get server config response</returns>
        public async Task<GetServerConfigResponse> GetServerConfigAsync(GetServerConfigRequest request)
        {
            var server = request.Server;
            if (server == null)
            {
                return null;
            }
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
            var configs = await redisServer.ConfigGetAsync("*").ConfigureAwait(false);
            if (configs == null || configs.Length <= 0)
            {
                return null;
            }
            CacheServerConfig config = new CacheServerConfig();

            #region 信息读取

            foreach (var cfg in configs)
            {
                string key = cfg.Key.ToLower();
                switch (key)
                {
                    case "daemonize":
                        config.Daemonize = cfg.Value.ToLower() == "yes";
                        break;
                    case "pidfile":
                        config.PidFile = cfg.Value;
                        break;
                    case "port":
                        int port = 0;
                        if (!int.TryParse(cfg.Value, out port))
                        {
                            port = 6379;
                        }
                        config.Port = port;
                        break;
                    case "bind":
                        config.Host = cfg.Value;
                        break;
                    case "timeout":
                        long timeOut = 0;
                        long.TryParse(cfg.Value, out timeOut);
                        config.TimeOut = timeOut;
                        break;
                    case "loglevel":
                        LogLevel logLevel = LogLevel.Verbose;
                        switch (cfg.Value)
                        {
                            case "debug":
                                logLevel = LogLevel.Debug;
                                break;
                            case "verbose":
                                logLevel = LogLevel.Verbose;
                                break;
                            case "notice":
                                logLevel = LogLevel.Notice;
                                break;
                            case "warning":
                                logLevel = LogLevel.Warning;
                                break;
                        }
                        config.LogLevel = logLevel;
                        break;
                    case "logfile":
                        config.LogFile = cfg.Value;
                        break;
                    case "databases":
                        int dataBaseCount = 0;
                        int.TryParse(cfg.Value, out dataBaseCount);
                        config.DataBase = dataBaseCount;
                        break;
                    case "save":
                        if (cfg.Value.IsNullOrEmpty())
                        {
                            continue;
                        }
                        var valueArray = cfg.Value.LSplit(" ");
                        List<DataChangeSaveConfig> saveInfos = new List<DataChangeSaveConfig>();
                        for (var i = 0; i < valueArray.Length; i += 2)
                        {
                            if (valueArray.Length <= i + 1)
                            {
                                break;
                            }
                            long seconds = 0;
                            long.TryParse(valueArray[i], out seconds);
                            long changes = 0;
                            long.TryParse(valueArray[i + 1], out changes);
                            saveInfos.Add(new DataChangeSaveConfig()
                            {
                                Seconds = seconds,
                                Changes = changes
                            });
                        }
                        config.SaveConfig = saveInfos;
                        break;
                    case "rdbcompression":
                        config.RdbCompression = cfg.Value.IsNullOrEmpty() ? true : cfg.Value.ToString() == "yes";
                        break;
                    case "dbfilename":
                        config.DbFileName = cfg.Value;
                        break;
                    case "dir":
                        config.DbDir = cfg.Value;
                        break;
                    case "slaveof":
                        if (cfg.Value.IsNullOrEmpty())
                        {
                            continue;
                        }
                        string[] masterArray = cfg.Value.LSplit(" ");
                        config.MasterHost = masterArray[0];
                        if (masterArray.Length > 1)
                        {
                            int masterPort = 0;
                            int.TryParse(masterArray[1], out masterPort);
                            config.MasterPort = masterPort;
                        }
                        else
                        {
                            config.MasterPort = 6379;
                        }
                        break;
                    case "masterauth":
                        config.MasterPwd = cfg.Value;
                        break;
                    case "requirepass":
                        config.Pwd = cfg.Value;
                        break;
                    case "maxclients":
                        int maxClient = 0;
                        int.TryParse(cfg.Value, out maxClient);
                        config.MaxClient = maxClient;
                        break;
                    case "maxmemory":
                        long maxMemory = 0;
                        long.TryParse(cfg.Value, out maxMemory);
                        config.MaxMemory = maxMemory;
                        break;
                    case "appendonly":
                        config.AppendOnly = cfg.Value.ToLower() == "yes";
                        break;
                    case "appendfilename":
                        config.AppendFileName = cfg.Value;
                        break;
                    case "appendfsync":
                        AppendfSync appendSync = AppendfSync.EverySecond;
                        switch (cfg.Value)
                        {
                            case "no":
                                appendSync = AppendfSync.No;
                                break;
                            case "always":
                                appendSync = AppendfSync.Always;
                                break;
                        }
                        config.AppendfSync = appendSync;
                        break;
                    case "vm-enabled":
                        config.EnabledVM = cfg.Value.ToLower() == "yes";
                        break;
                    case "vm-swap-file":
                        config.VMSwapFile = cfg.Value;
                        break;
                    case "vm-max-memory":
                        long vmMaxMemory = 0;
                        long.TryParse(cfg.Value, out vmMaxMemory);
                        config.VMMaxMemory = vmMaxMemory;
                        break;
                    case "vm-page-size":
                        int vmPageSize = 0;
                        int.TryParse(cfg.Value, out vmPageSize);
                        config.VMPageSize = vmPageSize;
                        break;
                    case "vm-pages":
                        long vmPages = 0;
                        long.TryParse(cfg.Value, out vmPages);
                        config.VMPages = vmPages;
                        break;
                    case "vm-max-threads":
                        int vmMaxThreads = 0;
                        int.TryParse(cfg.Value, out vmMaxThreads);
                        config.VMMaxThreads = vmMaxThreads;
                        break;
                    case "glueoutputbuf":
                        config.Glueoutputbuf = cfg.Value.ToLower() == "yes";
                        break;
                    case "activerehashing":
                        config.ActivereHashing = cfg.Value.ToLower() == "yes";
                        break;
                    case "include":
                        config.IncludeConfigFile = cfg.Value;
                        break;
                }
            }

            #endregion

            return new GetServerConfigResponse()
            {
                ServerConfig = config,
                Success = true
            };
        }

        #endregion

        #region save server config

        /// <summary>
        /// save server config
        /// </summary>
        /// <param name="request">request</param>
        /// <returns>save server config response</returns>
        public async Task<SaveServerConfigResponse> SaveServerConfigAsync(SaveServerConfigRequest request)
        {
            var config = request.ServerConfig;
            var server = request.Server;
            if (config == null)
            {
                return new SaveServerConfigResponse()
                {
                    Success = false,
                    Message = "server config is null"
                };
            }
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));

            #region 守护进程方式

            //redisServer.ConfigSet("daemonize", config.Daemonize.ToString().ToLower());

            #endregion

            #region 守护进程文件

            //redisServer.ConfigSet("pidfile", config.PidFile);

            #endregion

            #region 端口

            //if (config.Port > 0)
            //{
            //    redisServer.ConfigSet("port", config.Port);
            //}

            #endregion

            #region 服务地址

            //if (!config.Host.IsNullOrEmpty())
            //{
            //    redisServer.ConfigSet("bind", config.Host);
            //}

            #endregion

            #region 客户端连接超时时间

            if (config.TimeOut >= 0)
            {
                redisServer.ConfigSet("timeout", config.TimeOut);
            }

            #endregion

            #region 日志记录级别

            redisServer.ConfigSet("loglevel", config.LogLevel.ToString().ToLower());

            #endregion

            #region 日志记录方式

            //if (!config.LogFile.IsNullOrEmpty())
            //{
            //    redisServer.ConfigSet("logfile", config.LogFile);
            //}

            #endregion

            #region 数据库数量

            //if (config.DataBase > 0)
            //{
            //    redisServer.ConfigSet("databases",config.DataBase);
            //}

            #endregion

            #region 数据保存配置

            string saveConfigValue = string.Empty;
            if (!config.SaveConfig.IsNullOrEmpty())
            {
                List<string> configList = new List<string>();
                foreach (var saveCfg in config.SaveConfig)
                {
                    configList.Add(saveCfg.Seconds.ToString());
                    configList.Add(saveCfg.Changes.ToString());
                }
                saveConfigValue = string.Join(" ", configList);
            }
            redisServer.ConfigSet("save", saveConfigValue);

            #endregion

            #region 压缩保存数据

            redisServer.ConfigSet("rdbcompression", config.RdbCompression ? "yes" : "no");

            #endregion

            #region 数据库文件

            //if (!config.DbFileName.IsNullOrEmpty())
            //{
            //    redisServer.ConfigSet("dbfilename", config.DbFileName);
            //}

            #endregion

            #region 数据库存放目录

            if (!config.DbDir.IsNullOrEmpty())
            {
                redisServer.ConfigSet("dir", config.DbDir);
            }

            #endregion

            #region 主服务

            //if (!config.MasterHost.IsNullOrEmpty())
            //{
            //    string masterUrl = string.Format("{0} {1}",config.Host,config.Port>0?config.Port:6379);
            //    redisServer.ConfigSet("slaveof", masterUrl);
            //}

            #endregion

            #region 主服务密码

            if (config.MasterPwd != null)
            {
                redisServer.ConfigSet("masterauth", config.MasterPwd);
            }

            #endregion

            #region 连接密码

            if (config.Pwd != null)
            {
                redisServer.ConfigSet("requirepass", config.Pwd);
            }

            #endregion

            #region 最大客户端连接

            if (config.MaxClient >= 0)
            {
                redisServer.ConfigSet("maxclients", config.MaxClient);
            }

            #endregion

            #region 最大内存限制

            if (config.MaxMemory >= 0)
            {
                redisServer.ConfigSet("maxmemory", config.MaxMemory);
            }

            #endregion

            #region 每次更新都保存日志

            redisServer.ConfigSet("appendonly", config.AppendOnly ? "yes" : "no");

            #endregion

            #region 更新日志文件名

            //redisServer.ConfigSet("appendfilename",config.AppendFileName);

            #endregion

            #region 更新日志条件

            string appendfSyncVal = "everysec";
            switch (config.AppendfSync)
            {
                case AppendfSync.Always:
                    appendfSyncVal = "always";
                    break;
                case AppendfSync.EverySecond:
                    appendfSyncVal = "everysec";
                    break;
                case AppendfSync.No:
                    appendfSyncVal = "no";
                    break;
            }
            redisServer.ConfigSet("appendfsync", appendfSyncVal);

            #endregion

            #region 启用虚拟内存

            //redisServer.ConfigSet("vm-enabled", config.EnabledVM.ToString().ToLower());

            #endregion

            #region 虚拟内存文件名

            //redisServer.ConfigSet("vm-swap-file",config.VMSwapFile);

            #endregion

            #region 虚拟内存最大限制值

            //redisServer.ConfigSet("vm-max-memory",config.VMMaxMemory);

            #endregion

            #region swap文件pagesize

            //redisServer.ConfigSet("vm-page-size", config.VMMaxMemory);

            #endregion

            #region swap文件page数量

            //redisServer.ConfigSet("vm-pages", config.VMPages);

            #endregion

            #region 访问swap文件的线程数

            //if (config.VMMaxThreads > 0)
            //{
            //    redisServer.ConfigSet("vm-max-threads", config.VMMaxThreads);
            //}

            #endregion

            #region 合并小数据包

            //redisServer.ConfigSet("glueoutputbuf", config.Glueoutputbuf.ToString().ToLower());

            #endregion

            #region 激活重置哈希

            redisServer.ConfigSet("activerehashing", config.ActivereHashing ? "yes" : "no");

            #endregion

            #region 其它的配置文件

            //redisServer.ConfigSet("include", config.IncludeConfigFile);

            #endregion

            //重新将配置保存到配置文件
            await redisServer.ConfigRewriteAsync().ConfigureAwait(false);

            return new SaveServerConfigResponse()
            {
                Success = true
            };
        }

        #endregion

        #endregion

        #region Helper

        /// <summary>
        /// get database index
        /// </summary>
        /// <param name="server">cache server</param>
        /// <returns></returns>
        static int GetDbIndex(CacheServer server)
        {
            int dbIndex = -1;
            if (server == null)
            {
                return dbIndex;
            }
            if (!int.TryParse(server.Db, out dbIndex))
            {
                dbIndex = -1;
            }
            return dbIndex;
        }

        /// <summary>
        /// get database
        /// </summary>
        /// <param name="server">cache server</param>
        /// <returns></returns>
        static IDatabase GetDB(CacheServer server)
        {
            var connection = GetConnection(server);
            int dbIndex = GetDbIndex(server);
            IDatabase db = connection.GetDatabase(dbIndex);
            return db;
        }

        /// <summary>
        /// get connection
        /// </summary>
        /// <param name="server">server</param>
        /// <returns></returns>
        static ConnectionMultiplexer GetConnection(CacheServer server)
        {
            string serverKey = server.IdentityKey;
            if (connectionDict.TryGetValue(serverKey, out var multiplexer) && multiplexer != null)
            {
                return multiplexer;
            }
            var configOption = new ConfigurationOptions()
            {
                EndPoints =
                {
                    {
                        server.Host,
                        server.Port
                    }
                },
                AllowAdmin = server.AllowAdmin,
                ResolveDns = server.ResolveDns,
                Ssl = server.SSL
            };
            if (server.ConnectTimeout > 0)
            {
                configOption.ConnectTimeout = server.ConnectTimeout;
            }
            if (!server.Pwd.IsNullOrEmpty())
            {
                configOption.Password = server.Pwd;
            }
            if (!server.ClientName.IsNullOrEmpty())
            {
                configOption.ClientName = server.ClientName;
            }
            if (!server.SSLHost.IsNullOrEmpty())
            {
                configOption.SslHost = server.SSLHost;
            }
            if (server.SyncTimeout > 0)
            {
                configOption.SyncTimeout = server.SyncTimeout;
            }
            if (!server.TieBreaker.IsNullOrEmpty())
            {
                configOption.TieBreaker = server.TieBreaker;
            }
            multiplexer = ConnectionMultiplexer.Connect(configOption);
            connectionDict.TryAdd(serverKey, multiplexer);
            return multiplexer;
        }

        /// <summary>
        /// serialize data
        /// </summary>
        /// <param name="obj">obj</param>
        /// <returns>byte values</returns>
        static byte[] Serialize(object obj)
        {
            if (obj == null)
            {
                return null;
            }
            Type objectType = obj.GetType();
            string value = string.Empty;
            if (DataConverter.IsSimpleType(objectType))
            {
                value = obj.ToString();
            }
            else
            {
                value = JsonSerialize.ObjectToJson(obj);
            }
            return Encoding.UTF8.GetBytes(value);
        }

        /// <summary>
        /// deserialize data
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="stream">data stream</param>
        /// <returns>data object</returns>
        static T Deserialize<T>(byte[] stream)
        {
            if (stream == null || stream.Length <= 0)
            {
                return default(T);
            }
            string value = Encoding.UTF8.GetString(stream);
            Type type = typeof(T);
            if (DataConverter.IsSimpleType(type))
            {
                return DataConverter.ConvertToSimpleType<T>(value);
            }
            return JsonSerialize.JsonToObject<T>(value);
        }

        /// <summary>
        /// get hash values
        /// </summary>
        /// <param name="value">object</param>
        /// <returns></returns>
        static List<HashEntry> GetHashValues(object value)
        {
            if (value == null)
            {
                return new List<HashEntry>(0);
            }
            List<HashEntry> hashDataList = new List<HashEntry>();
            IDictionary<string, dynamic> valueDic = null;
            if (value is IDictionary<string, string>)
            {
                valueDic = value as IDictionary<string, dynamic>;
            }
            else
            {
                valueDic = value.ObjectToDcitionary();
            }
            if (valueDic == null)
            {
                return new List<HashEntry>(0);
            }
            foreach (var valItem in valueDic)
            {
                hashDataList.Add(new HashEntry(valItem.Key, valItem.Value.ToString()));
            }
            return hashDataList;
        }

        static Dictionary<string, string> HashEntryConvertToDictionary(IEnumerable<HashEntry> hashEntrys)
        {
            if (hashEntrys == null || !hashEntrys.Any())
            {
                return new Dictionary<string, string>(0);
            }
            Dictionary<string, string> values = new Dictionary<string, string>();
            foreach (var hashData in hashEntrys)
            {
                if (values.ContainsKey(hashData.Name))
                {
                    values[hashData.Name] = hashData.Value;
                }
                else
                {
                    values.Add(hashData.Name, hashData.Value);
                }
            }
            return values;
        }

        /// <summary>
        /// get set opeartion
        /// </summary>
        /// <param name="operationType">operation type</param>
        /// <returns></returns>
        static SetOperation GetSetOperation(SetOperationType operationType)
        {
            SetOperation operation = SetOperation.Union;
            switch (operationType)
            {
                case SetOperationType.Union:
                default:
                    break;
                case SetOperationType.Difference:
                    operation = SetOperation.Difference;
                    break;
                case SetOperationType.Intersect:
                    operation = SetOperation.Intersect;
                    break;
            }
            return operation;
        }

        /// <summary>
        /// sorted set data convert
        /// </summary>
        /// <returns></returns>
        SortedSetEntry[] SetValueItemConvertToSortedSetEntry(IEnumerable<SortedSetValueItem> values)
        {
            if (values == null || !values.Any())
            {
                return null;
            }
            return values.Select(c => new SortedSetEntry(c.Value, c.Score)).ToArray();
        }

        /// <summary>
        /// get set exclude
        /// </summary>
        /// <param name="setExclude"></param>
        /// <returns></returns>
        Exclude GetExclude(SortedSetExclude setExclude)
        {
            Exclude exclude = Exclude.None;
            switch (setExclude)
            {
                case SortedSetExclude.None:
                default:
                    break;
                case SortedSetExclude.Start:
                    exclude = Exclude.Start;
                    break;
                case SortedSetExclude.Both:
                    exclude = Exclude.Both;
                    break;
                case SortedSetExclude.Stop:
                    exclude = Exclude.Stop;
                    break;
            }
            return exclude;
        }

        /// <summary>
        /// get sorted order
        /// </summary>
        /// <param name="sortedOrder"></param>
        /// <returns></returns>
        Order GetSortedOrder(SortedOrder sortedOrder)
        {
            Order order = Order.Ascending;
            switch (sortedOrder)
            {
                case SortedOrder.Ascending:
                default:
                    break;
                case SortedOrder.Descending:
                    order = Order.Descending;
                    break;
            }
            return order;
        }

        /// <summary>
        /// get command flags
        /// </summary>
        /// <param name="cacheCommandFlags">cache command flags</param>
        /// <returns>command flags</returns>
        CommandFlags GetCommandFlags(CacheCommandFlags cacheCommandFlags)
        {
            CommandFlags cmdFlags = CommandFlags.None;
            switch (cacheCommandFlags)
            {
                case CacheCommandFlags.None:
                default:
                    cmdFlags = CommandFlags.None;
                    break;
                case CacheCommandFlags.DemandMaster:
                    cmdFlags = CommandFlags.DemandMaster;
                    break;
                case CacheCommandFlags.DemandSlave:
                    cmdFlags = CommandFlags.DemandSlave;
                    break;
                case CacheCommandFlags.FireAndForget:
                    cmdFlags = CommandFlags.FireAndForget;
                    break;
                case CacheCommandFlags.HighPriority:
                    cmdFlags = CommandFlags.HighPriority;
                    break;
                case CacheCommandFlags.NoRedirect:
                    cmdFlags = CommandFlags.NoRedirect;
                    break;
                case CacheCommandFlags.NoScriptCache:
                    cmdFlags = CommandFlags.NoScriptCache;
                    break;
                case CacheCommandFlags.PreferSlave:
                    cmdFlags = CommandFlags.PreferSlave;
                    break;
            }
            return cmdFlags;
        }

        /// <summary>
        /// get Redis When enum value
        /// </summary>
        /// <param name="cacheWhen">CacheWhen enum value</param>
        /// <returns></returns>
        When GetWhen(CacheWhen cacheWhen)
        {
            When when = When.Always;
            switch (cacheWhen)
            {
                case CacheWhen.Always:
                default:
                    when = When.Always;
                    break;
                case CacheWhen.Exists:
                    when = When.Exists;
                    break;
                case CacheWhen.NotExists:
                    when = When.NotExists;
                    break;
            }
            return when;
        }

        /// <summary>
        /// get bitwise
        /// </summary>
        /// <param name="cacheBitwise">cache bit wise</param>
        /// <returns></returns>
        Bitwise GetBitwise(CacheBitwise cacheBitwise)
        {
            Bitwise bitwise = Bitwise.And;
            switch (cacheBitwise)
            {
                case CacheBitwise.And:
                default:
                    bitwise = Bitwise.And;
                    break;
                case CacheBitwise.Not:
                    bitwise = Bitwise.Not;
                    break;
                case CacheBitwise.Or:
                    bitwise = Bitwise.Or;
                    break;
                case CacheBitwise.Xor:
                    bitwise = Bitwise.Xor;
                    break;
            }
            return bitwise;
        }

        /// <summary>
        /// get aggregate
        /// </summary>
        /// <param name="setAggregate"></param>
        /// <returns></returns>
        Aggregate GetAggregate(SetAggregate setAggregate)
        {
            Aggregate aggregate = Aggregate.Sum;
            switch (setAggregate)
            {
                case SetAggregate.Sum:
                default:
                    aggregate = Aggregate.Sum;
                    break;
                case SetAggregate.Max:
                    aggregate = Aggregate.Max;
                    break;
                case SetAggregate.Min:
                    aggregate = Aggregate.Min;
                    break;
            }
            return aggregate;
        }

        /// <summary>
        /// get sort type
        /// </summary>
        /// <param name="cacheSortType"></param>
        /// <returns></returns>
        SortType GetSortType(CacheSortType cacheSortType)
        {
            SortType sortType = SortType.Numeric;
            switch (cacheSortType)
            {
                case CacheSortType.Numeric:
                default:
                    sortType = SortType.Numeric;
                    break;
                case CacheSortType.Alphabetic:
                    sortType = SortType.Alphabetic;
                    break;
            }
            return sortType;
        }

        /// <summary>
        /// get cache key type
        /// </summary>
        /// <param name="redisType"></param>
        /// <returns></returns>
        CacheKeyType GetCacheKeyType(RedisType redisType)
        {
            CacheKeyType keyType = CacheKeyType.String;
            switch (redisType)
            {
                case RedisType.String:
                default:
                    keyType = CacheKeyType.String;
                    break;
                case RedisType.List:
                    keyType = CacheKeyType.List;
                    break;
                case RedisType.Hash:
                    keyType = CacheKeyType.Hash;
                    break;
                case RedisType.Set:
                    keyType = CacheKeyType.Set;
                    break;
                case RedisType.SortedSet:
                    keyType = CacheKeyType.SortedSet;
                    break;
            }
            return keyType;
        }

        /// <summary>
        /// get migrateoptions
        /// </summary>
        /// <param name="cacheMigrateOptions"></param>
        /// <returns></returns>
        MigrateOptions GetMigrateOptions(CacheMigrateOptions cacheMigrateOptions)
        {
            MigrateOptions migrateOption = MigrateOptions.None;
            switch (cacheMigrateOptions)
            {
                case CacheMigrateOptions.None:
                default:
                    migrateOption = MigrateOptions.None;
                    break;
                case CacheMigrateOptions.Copy:
                    migrateOption = MigrateOptions.Copy;
                    break;
                case CacheMigrateOptions.Replace:
                    migrateOption = MigrateOptions.Replace;
                    break;
            }
            return migrateOption;
        }

        #endregion
    }
}
