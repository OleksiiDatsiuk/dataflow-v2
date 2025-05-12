package org.arpha.http.routing;

import java.util.Map;

public record RouteMatch(RouteDefinition route, Map<String, String> pathParams) {
}
