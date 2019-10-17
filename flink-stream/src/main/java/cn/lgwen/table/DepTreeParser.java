package cn.lgwen.table;

import lombok.Data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

/*
 *@author jibiyr@qq.com
 */
public class DepTreeParser {

    public static void main(String[] args) throws Exception {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(Thread.currentThread().getContextClassLoader().getResource("dep.tree").getFile()));

        List<Dep> deps = new ArrayList<>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            int levelEndIndex = line.indexOf("]");
            line = line.substring(levelEndIndex + 1).trim();
            char[] chars = line.toCharArray();
            int level = 0;
            for (int i = 0; i < chars.length; i++) {
                char ch = chars[i];
                if (ch >= 'a' && ch <= 'z') {
                    level = i;
                    break;
                }
            }
            String[] depArrays = line.substring(level).split(":");
            deps.add(new Dep(level, depArrays));
        }

        Dep dep = makeTree(deps);
        Map<String, Dep> sonMap = dep.getGroupArtMap();
        final Integer[] i = {0};
        Map<String, Set<String>> all = new HashMap<>();
        sonMap.forEach((k, v) -> {
            Set<String> strings = new HashSet<>();
            v.makeExcludeRecurseWithOutIndent(strings);
            all.put(k, strings);
        });
        Set<String> include = new HashSet<>();
        include.addAll(all.remove("org.elasticsearch.client:elasticsearch-rest-high-level-client"));
        include.addAll(all.remove("org.slf4j:slf4j-api"));
        include.addAll(all.remove("org.slf4j:slf4j-log4j12"));
        include.addAll(all.remove("com.alibaba:fastjson"));
        include.addAll(all.remove("org.apache.logging.log4j:log4j-core"));
        include.addAll(all.remove("com.dbappsecurity.cpsysportal:util"));
        include.addAll(all.remove("org.projectlombok:lombok"));
        include.addAll(all.remove("org.apache.logging.log4j:log4j-api"));
        Collection<Set<String>> exclude = all.values();
        exclude.forEach(ex -> {
                    ex.removeAll(include);
                }
        );
        Set<String> result = new HashSet<>();
        exclude.forEach(result::addAll);
        result.forEach(System.out::println);

        System.out.println(dep);
    }

    private static Dep makeTree(List<Dep> deps) {
        Dep result = deps.get(0);
        Dep last = result;
        //boolean lastSonEnd = false;
        int lastLevel = 0;
        LinkedList<Dep> stack = new LinkedList<>();
        stack.push(last);
        for (int i = 1; i < deps.size(); i++) {
            Dep dep = deps.get(i);
            last = stack.pop();
            if (dep.level > lastLevel && lastLevel != 0) {
                //相当于一次函数调用，先压栈，记录
                stack.push(last);
                last.addSon(dep);
                //然后在压栈新的函数调用
                last = dep;
                stack.push(last);
            } else if (dep.level < lastLevel) {
                //相当于函数调用结束；我们一直pop 到一个和当前level相等的，然后用当前的替换
                while ((last = stack.pop()).level != dep.level) {
                }
                i--;
            } else {
                if (last.level != dep.level) {
                    stack.push(last);
                } else {
                    last = stack.pop();
                    stack.push(last);
                }
                last.addSon(dep);
                last = dep;
                stack.push(last);
            }
            lastLevel = dep.level;
        }
        checkResult(result);
        Set<Dep> set = new HashSet<>();
        visit(result, set);
        return result;
    }

    private static void visit(Dep result, Set<Dep> set) {
        if (result.son.size() == 0) {
        } else {
            result.son.forEach(r -> {
                visit(r, set);
                set.add(r);

            });
        }
    }

    private static void checkResult(Dep result) {
        if (result.son == null) {
            return;
        } else {
            result.son.forEach(dep -> {
                if (result.level >= dep.level) {
                    throw new IllegalStateException();
                }
                checkResult(dep);
            });
        }
    }

    @Data
    static class DepGraph {

    }

    @Data
    static class Dep {
        int level;
        String group;
        String art;
        String version;
        String scope;

        List<Dep> son = new ArrayList<>();


        public Dep(int level, String[] depArrays) {
            this.level = level;
            for (int i = 0; i < depArrays.length; i++) {
                String item = depArrays[i];
                if (i == 0) {
                    group = item;
                } else if (i == 1) {
                    art = item;
                } else if (i == 2) {
                    version = item;
                } else if (i == 3) {
                    version = item;
                }
            }
        }

        public Map<String, Dep> getSonMap() {
            if (son == null) {
                return null;
            }

            return son.stream().collect(Collectors.toMap(Dep::getKey, a -> a));
        }

        public Map<String, Dep> getGroupArtMap() {
            if (son == null) {
                return null;
            }

            return son.stream().collect(Collectors.toMap(Dep::getGroupArt, a -> a));
        }

        public String makeExclude(boolean retract) {
            StringBuilder levelBlank = new StringBuilder("");
            if (retract) {
                for (int i = 0; i < level; i++) {
                    levelBlank.append(" ");
                }
            }
            return levelBlank.append("<exclude>").append(getGroupArt()).append("</exclude>").toString();
        }

        public void makeExcludeRecurse(final Set<String> strings) {
            strings.add(makeExclude(true));
            if (son.size() == 0) {
            } else {
                son.forEach(s -> {
                    s.makeExcludeRecurse(strings);
                });
            }
        }

        public void makeExcludeRecurseWithOutIndent(final Set<String> strings) {
            strings.add(makeExclude(false));
            if (son.size() == 0) {
            } else {
                son.forEach(s -> {
                    s.makeExcludeRecurseWithOutIndent(strings);
                });
            }
        }

        private String getKey() {
            return group + ":" + art + ":" + version;
        }

        private String getGroupArt() {
            return group + ":" + art;
        }

        @Override
        public String toString() {
            return "Dep{" +
                    "level=" + level +
                    ", group=" + group +
                    ", art=" + art +
                    ", version=" + version +
                    ", scope=" + scope +
                    '}';
        }

        public void addSon(Dep dep) {
            if (dep.level == this.level) {
                throw new IllegalStateException();
            }
            son.add(dep);
        }
    }
}
