package FlinkCommerce.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    public static <T> String convertObjectToJson(T element){
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
