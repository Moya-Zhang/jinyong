
public class alltask {
    public static void main(String[] args)
            throws Exception
    {
        String[] arg1={args[0],args[1]+"out1"};
        String[] arg2={args[1]+"out1",args[1]+"out2"};
        String[] arg3={args[1]+"out2",args[1]+"out3"};
        String[] arg4={args[1]+"out3",args[1]+"prResult"};
        String[] arg5={args[1]+"out3", args[1]+"lpaResult"};
        task1.main(arg1);
        task2.main(arg2);
        task3.main(arg3);
        task4.main(arg4);
        task5.main(arg5);
    }
}
