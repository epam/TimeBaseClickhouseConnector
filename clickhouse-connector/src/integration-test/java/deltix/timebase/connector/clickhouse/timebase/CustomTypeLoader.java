package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.TypeLoader;
import deltix.qsrv.hf.pub.md.ClassDescriptor;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.timebase.messages.SchemaElement;

import java.util.HashMap;

public class CustomTypeLoader implements TypeLoader {
    private final HashMap<String, Class> resolvedTypes = new HashMap<>();
    private TypeLoader defaultLoader;

    /**
     * Constructor. Creates a new TypeLoader instance.
     */
    public CustomTypeLoader() {
    }

    /**
     * Constructor. Creates a new TypeLoader instance.
     *
     * @param defaultLoader Default type loader.
     */
    public CustomTypeLoader(TypeLoader defaultLoader) {
        this.defaultLoader = defaultLoader;
    }

    /**
     * Constructor. Creates a new TypeLoader instance and registers default set of resolved types.
     *
     * @param types Default set of resolved types.
     */

    public CustomTypeLoader(java.lang.Class... types) {
        addTypes(types);
    }

    private static ClassDescriptor getParent(ClassDescriptor childDescriptor) {
        RecordClassDescriptor recordClassDescriptor = childDescriptor instanceof RecordClassDescriptor ? (RecordClassDescriptor) childDescriptor : null;
        return recordClassDescriptor != null ? recordClassDescriptor.getParent() : null;
    }

    /**
     * Default type loader. If initialized, it will be used when current instance of type loader cannot resolve a ClassDescriptor.
     */
    public final TypeLoader getDefaultLoader() {
        return defaultLoader;
    }

    /**
     * Registers a runtime type. This type will be resolved by a TypeLoader.
     * If type is tagged with a <see cref="TimebaseEntityAttribute"/> attribute, the record class name will be taken from attribute value. Otherwise, full class name will be used.
     * Method is not thread safe. If multi-thread usage is required, external synchronization must be provided.
     *
     * @param type Runtime type to be used for communication with timebase.
     */
    public final void addType(java.lang.Class type) {
        if (type == null) {
            throw new NullPointerException("type");
        }

        addType(getDescriptorName(type), type);
    }

    /**
     * Registers a runtime type. This type will be resolved by a TypeLoader.
     * Method is not thread safe. If multi-thread usage is required, external synchronization must be provided.
     *
     * @param descriptorName The name of a record class.
     * @param type           Runtime type to be used for communication with timebase.
     */
    public final void addType(String descriptorName, java.lang.Class type) {
        if (type == null) {
            throw new NullPointerException("type");
        }

        java.lang.Class currentType = resolvedTypes.get(descriptorName);
        if (currentType != null) {
            if (type != currentType) {
                throw new IllegalArgumentException(String.format("Cannot map type '%s' to descriptor '%s' because it is already mapped to a type '%s'.", type.getName(), descriptorName, currentType.getName()));
            }

            return; // Type already added.
        }

        resolvedTypes.put(descriptorName, type);
    }

    /**
     * Registers assembly. All public types in assembly except for abstract and static will be resolved.
     * Method is not thread safe. If multi-thread usage is required, external synchronization must be provided.
     *
     * @param assembly                    Instance of assembly.
     * @param resolveTimebaseEntitiesOnly Specifies if only classes and enums tagged with a <see cref="TimebaseEntityAttribute"/> attribute should be resolved.
     */

    /**
     * Registers an arbitrary number of runtime types.
     * Method is not thread safe. If multi-thread usage is required, external synchronization must be provided.
     *
     * @param types A collection of runtime types to be used for communication with timebase.
     */
    @SuppressWarnings("unchecked")
    public final void addTypes(java.lang.Class... types) {
        if (types == null) {
            throw new NullPointerException("types");
        }

        for (java.lang.Class type : types) {
            addType(type);
        }
    }

    /**
     * Resolves a runtime type for a record class. Called by timebase codecs.
     *
     * @param classDescriptor Descriptor to be resolved.
     * @return A runtime class for a record class.
     */
    public Class load(ClassDescriptor classDescriptor) throws ClassNotFoundException {
        ClassDescriptor currentDescriptor = classDescriptor;

        while (currentDescriptor != null) {
            String recordClassName = currentDescriptor.getName();

            if (resolvedTypes.containsKey(recordClassName)) return resolvedTypes.get(recordClassName);
            currentDescriptor = getParent(currentDescriptor);
        }

        if (defaultLoader != null) {
            return defaultLoader.load(classDescriptor);
        }

        return null;
    }

    /**
     * Gets the name of record class descriptor.
     * If type is tagged with a <see cref="TimebaseEntityAttribute"/> attribute, the record class name will be taken from attribute value. Otherwise, full class name will be used.
     *
     * @param type Runtime type to be used for communication with timebase.
     * @return The name of record class descriptor for type.
     */
    public static String getDescriptorName(java.lang.Class type) {
        SchemaElement classNameAttribute = (SchemaElement) type.getAnnotation(SchemaElement.class);
        return classNameAttribute != null && classNameAttribute.name() != null && classNameAttribute.name().length() > 0 ? classNameAttribute.name() : type.getName();
    }
}
