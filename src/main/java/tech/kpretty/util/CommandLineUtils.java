package tech.kpretty.util;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.IOException;

/**
 * @author wjun
 * @date 2022/6/1 11:15
 * @email wj2247689442@gmail.com
 * @describe 命令行工具类
 */
public class CommandLineUtils {
    public static void checkRequest(OptionParser parser, OptionSet parse, ArgumentAcceptingOptionSpec... options) throws IOException {
        for (ArgumentAcceptingOptionSpec option : options) {
            // option 是必须的同时这个 option 不存在
            if (option.requiresArgument() && !parse.has(option)) {
                System.out.println("Miss request args: " + option.options().get(0));
                parser.printHelpOn(System.out);
                System.exit(-1);
            }
        }
    }
}
