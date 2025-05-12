package org.arpha.http.routing;

import org.arpha.http.annotation.HttpRoute;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Router {

    private final List<RouteDefinition> routes = new ArrayList<>();

    public void registerController(Object controller) {
        for (Method method : controller.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(HttpRoute.class)) {
                HttpRoute routeAnnotation = method.getAnnotation(HttpRoute.class);
                PathPattern pp = compilePathPattern(routeAnnotation.path());
                RouteDefinition route = new RouteDefinition(
                        routeAnnotation.path(),
                        routeAnnotation.method().toUpperCase(),
                        controller,
                        method,
                        pp.getPattern(),
                        pp.getParamNames()
                );
                routes.add(route);
            }
        }
    }

    /**
     * Try to find a matching route for the given URI and HTTP method.
     * Returns an Optional with RouteMatch if a match is found, otherwise empty.
     */
    public Optional<RouteMatch> findRoute(String uri, String httpMethod) {
        for (RouteDefinition route : routes) {
            if (!route.httpMethod().equalsIgnoreCase(httpMethod)) {
                continue;
            }
            Matcher matcher = route.pattern().matcher(uri);
            if (matcher.matches()) {
                Map<String, String> pathParams = new HashMap<>();
                List<String> paramNames = route.pathParamNames();
                for (int i = 0; i < paramNames.size(); i++) {
                    String value = matcher.group(i + 1);
                    pathParams.put(paramNames.get(i), value);
                }
                return Optional.of(new RouteMatch(route, pathParams));
            }
        }
        return Optional.empty();
    }

    public static class PathPattern {
        private final Pattern pattern;
        private final List<String> paramNames;

        public PathPattern(Pattern pattern, List<String> paramNames) {
            this.pattern = pattern;
            this.paramNames = paramNames;
        }

        public Pattern getPattern() {
            return pattern;
        }

        public List<String> getParamNames() {
            return paramNames;
        }
    }

    private static PathPattern compilePathPattern(String template) {
        List<String> paramNames = new ArrayList<>();
        StringBuilder regexBuilder = new StringBuilder();
        int start = 0;
        Pattern p = Pattern.compile("\\{([^/]+?)\\}");
        Matcher m = p.matcher(template);
        while (m.find()) {
            regexBuilder.append(Pattern.quote(template.substring(start, m.start())));
            regexBuilder.append("([^/]+)");
            paramNames.add(m.group(1));
            start = m.end();
        }
        regexBuilder.append(Pattern.quote(template.substring(start)));
        Pattern pattern = Pattern.compile("^" + regexBuilder + "$");
        return new PathPattern(pattern, paramNames);
    }

    public List<RouteDefinition> getRoutes() {
        return routes;
    }

}

