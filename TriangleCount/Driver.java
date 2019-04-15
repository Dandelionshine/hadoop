
public class Driver
{
    public static void main(String[]args)
            throws Exception
    {
        String forCD[]={args[0],args[1]};
        CreatUndirectGraph.main(forCD);
        String forCL[]={args[1],args[2]};
        CreatLink.main(forCL);
    }
}
