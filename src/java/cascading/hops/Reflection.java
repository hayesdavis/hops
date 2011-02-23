package cascading.hops;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Reflection {
    
    public static <T> T newInstance(Class<T> clazz, Object... args)
        throws IllegalArgumentException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
        Class[] paramTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            paramTypes[i] = args[i].getClass();
        }
        Constructor<T> cons = getConstructor(clazz, paramTypes);
        return cons.newInstance(args);
    }

    private static Constructor getConstructor(Class type, Class[] params) {
        for(Constructor cons : type.getConstructors()) {
            if(isSignatureMatch(cons.getParameterTypes(),params)) {
                return cons;
            }
        }
        return null;
    }
    
    private static boolean isSignatureMatch(Class[] declaredTypes, Class[] argTypes) {
        if(declaredTypes.length != argTypes.length) {
            return false;
        }
        for(int i=0; i<declaredTypes.length; i++) {
            if(!declaredTypes[i].isAssignableFrom(argTypes[i])) {
                return false;
            }
        }
        return true;
    }
    
}
