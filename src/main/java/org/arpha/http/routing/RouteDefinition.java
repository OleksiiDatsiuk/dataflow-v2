package org.arpha.http.routing;

import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Pattern;

public record RouteDefinition(String originalPath, String httpMethod, Object controller, Method handlerMethod,
                              Pattern pattern, List<String> pathParamNames) {

}
