""" Utilities for pretty printing table data and more
    Author: A.Tsaregorodtsev
"""

from io import StringIO


def int_with_commas(inputValue):
    """Utility to make a string of a large integer with comma separated degrees
        of thousand

    :param int inputValue: value to be interpreted
    :return: output string
    """
    s = str(inputValue)
    news = ""
    while len(s) > 0:
        news = s[-3:] + "," + news
        s = s[:-3]
    return news[:-1]


def printTable(fields, records, sortField="", numbering=True, printOut=True, columnSeparator=" "):
    """Utility to pretty print tabular data

    :param list fields: list of column names
    :param list records: list of records, each record is a list or tuple of field values
           where field value itself can be a string or a list of strings or a dictionary
           of the structure { "Value": string_value, 'Just': 'L|R|C' } to specify justification
    :param str sortField: name of the column by which the output will be sorted
    :param bool numbering: flag for numbering rows
    :param bool printOut: flag for printing into the stdout
    :param str columnSeparator: string to be used as a column separator
    :return: pretty table string
    """

    def __writeField(buffer, value, length, columnSeparator, lastColumn=False):
        justification = None
        if isinstance(value, dict):
            justification = value.get("Just")
            value = value.get("Value", "")

        if justification is None:
            # try casting to int and then align to the right, if it fails align to the left
            justification = "r"
            try:
                _ = int("".join(value.split(",")))
            except ValueError:
                justification = "l"

        if justification.lower() == "l":
            output = value.ljust(length) + columnSeparator
        elif justification.lower() == "r":
            output = value.rjust(length) + columnSeparator
        elif justification.lower() == "c":
            margin = length - len(value)
            if margin <= 1:
                output = value.ljust(length) + columnSeparator
            else:
                m1 = int(margin / 2)
                m2 = margin - m1
                output = " " * m1 + value + " " * m2 + columnSeparator
        if lastColumn:
            output = output.rstrip()
        buffer.write(output)
        return len(output)

    if not records:
        if printOut:
            print("No output")
        return "No output"

    # Strip all strings
    fieldList = [f.strip() for f in fields]
    recordList = []
    for record in records:
        strippedRecord = []
        for fieldValue in record:
            if isinstance(fieldValue, str):
                strippedRecord.append(fieldValue.strip())
            elif isinstance(fieldValue, list):
                strippedList = []
                for ll in fieldValue:
                    if isinstance(ll, str):
                        strippedList.append(ll.strip())
                    elif isinstance(ll, dict):
                        ll["Value"] = ll["Value"].strip()
                        strippedList.append(ll)
                    else:
                        out = f"Wrong type for field value: {type(ll)}"
                        if printOut:
                            print(out)
                        return out
                strippedRecord.append(strippedList)
            elif isinstance(fieldValue, dict):
                itemValue = fieldValue["Value"]
                if isinstance(itemValue, str):
                    itemValue = itemValue.strip()
                    fieldValue.update({"Value": itemValue})
                    strippedRecord.append(fieldValue)
                elif isinstance(itemValue, list):
                    itemValue = [r.strip() for r in itemValue]
                    fieldValue.update({"Value": itemValue})
                    strippedRecord.append(fieldValue)
                else:
                    out = f"Wrong type for field value: {type(itemValue)}"
                    if printOut:
                        print(out)
                    return out
        recordList.append(strippedRecord)

    nFields = len(fields)
    for rec in records:
        if nFields != len(rec):
            out = "Incorrect data structure to print, nFields %d, nRecords %d" % (nFields, len(rec))
            if printOut:
                print(out)
            return out

    if sortField:
        recordList.sort(key=lambda x: x[fieldList.index(sortField)])

    # Compute the maximum width for each field
    fieldWidths = []
    for i in range(nFields):
        fieldWidths.append(len(fieldList[i]))
        for record in recordList:
            if isinstance(record[i], list):
                for item in record[i]:
                    if isinstance(item, dict):
                        itemValue = item["Value"]
                    else:
                        itemValue = item
                    fieldWidths[i] = max(len(itemValue), fieldWidths[i])
            elif isinstance(record[i], dict):
                fieldValue = record[i]["Value"]
                if isinstance(fieldValue, list):
                    fieldWidths[i] = max(max(len(item) for item in fieldValue), fieldWidths[i])
                else:
                    fieldWidths[i] = max(len(fieldValue), fieldWidths[i])
            else:
                fieldWidths[i] = max(len(record[i]), fieldWidths[i])

    numberWidth = len(str(len(recordList))) + 1
    separatorWidth = len(columnSeparator)
    totalLength = sum(fieldWidths) + separatorWidth * nFields
    if numbering:
        totalLength += numberWidth + separatorWidth

    # Accumulate the table output in the stringBuffer now
    stringBuffer = StringIO()
    topLength = (numberWidth + separatorWidth) if numbering else 0
    stringBuffer.write(" " * (topLength))

    for i in range(nFields):
        lastColumn = False if i < nFields - 1 else True
        length = __writeField(stringBuffer, fieldList[i], fieldWidths[i], columnSeparator, lastColumn)
        topLength += length
    stringBuffer.write("\n")
    if columnSeparator == " ":
        stringBuffer.write("=" * topLength + "\n")
    else:
        stringBuffer.write("=" * totalLength + "\n")

    for count, record in enumerate(recordList):
        total = count == len(recordList) - 1 and recordList[-1][0] == "Total"
        if numbering:
            if total:
                # Do not number the line with the total
                stringBuffer.write(" " * (numberWidth + separatorWidth))
            else:
                stringBuffer.write(str(count + 1).rjust(numberWidth) + columnSeparator)

        listMode = 0
        for item in record:
            if isinstance(item, list):
                listMode = max(len(item), listMode)
            elif isinstance(item, dict) and isinstance(item["Value"], list):
                listMode = max(len(item["Value"]), listMode)

        for i, (fieldValue, fieldWidth) in enumerate(zip(record, fieldWidths)):
            lastColumn = False if i < nFields - 1 else True
            value = fieldValue
            if isinstance(fieldValue, list):
                value = fieldValue[0]
            elif isinstance(fieldValue, dict) and isinstance(fieldValue["Value"], list):
                value = dict(fieldValue)
                value.update({"Value": value["Value"][0]})

            __writeField(stringBuffer, value, fieldWidth, columnSeparator, lastColumn)

        # If the field has a list type value, print out one value per line
        if listMode:
            stringBuffer.write("\n")
            for ll in range(1, listMode):
                # Do not number continuation lines
                if numbering:
                    stringBuffer.write(" " * (numberWidth + separatorWidth))
                for i, (fieldValue, fieldWidth) in enumerate(zip(record, fieldWidths)):
                    lastColumn = False if i < nFields - 1 else True
                    valueList = fieldValue
                    if isinstance(valueList, list) and ll < len(valueList):
                        value = valueList[ll]
                    elif (
                        isinstance(valueList, dict)
                        and isinstance(valueList["Value"], list)
                        and ll < len(valueList["Value"])
                    ):
                        value = dict(valueList)
                        value.update({"Value": valueList["Value"][ll]})
                    else:
                        value = ""

                    __writeField(stringBuffer, value, fieldWidth, columnSeparator, lastColumn)

                stringBuffer.write("\n")

        if not listMode:
            stringBuffer.write("\n")
        if total:
            stringBuffer.write("-" * totalLength + "\n")

    output = stringBuffer.getvalue()
    if printOut:
        print(output)

    return output


def printDict(dDict, printOut=False):
    """Utility to pretty print a dictionary

    :param dict dDict: Dictionary to be printed out
    :param bool printOut: flag to print to the stdout
    :return: pretty dictionary string
    """
    lines = []
    keyLength = 0
    for key in dDict:
        if len(key) > keyLength:
            keyLength = len(key)
    for key in sorted(dDict):
        line = f"{key}: "
        line = line.ljust(keyLength + 2)
        value = dDict[key]
        if isinstance(value, (list, tuple)):
            line += ",".join(list(value))
        else:
            line += str(value)
        lines.append(line)
    output = "{\n%s\n}" % "\n".join(lines)
    if printOut:
        print(output)
    return output
