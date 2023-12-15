package thesis.pet;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A class that delivers an initialized PET function.
 * Reserved for future work: this class is also responsible for fetching a PET, which is locally unavailable, from a trusted remote repository
 */
public class PETProvider {


    /**
     * A static method to build the desired PET algorithm according to its description.
     * @param descriptor The description of the PET processed by {@link PETDescriptor}
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    public static PETFragment build(PETDescriptor descriptor)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        if (descriptor==null) throw new RuntimeException("Descriptor is null");
        Class<?> fragmentClass = Class.forName(descriptor.getName());

        // Order of the parameters should match
        Constructor<?> constructor = fragmentClass.getDeclaredConstructor(descriptor.getConstructorClasses());

        constructor.setAccessible(true);
        return (PETFragment) constructor.newInstance(descriptor.getParameterWithInitialValues());

    }


}
