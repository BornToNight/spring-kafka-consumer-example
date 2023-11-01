package ru.demo.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.demo.model.StringValue

@Service
class StringValueConsumerLogger : StringValueConsumer {
    override fun accept(valueList: List<StringValue>) {
        for (value in valueList) {
            log.info("log:{}", value)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(StringValueConsumerLogger::class.java)
    }
}
