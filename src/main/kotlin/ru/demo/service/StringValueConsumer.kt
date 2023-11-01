package ru.demo.service

import ru.demo.model.StringValue

interface StringValueConsumer {

    fun accept(valueList: List<StringValue>)

}