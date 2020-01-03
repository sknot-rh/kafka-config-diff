import java.util.ArrayList;
import java.util.Properties;

public class KafkaConfigurationDiff {
    private String current;
    private String desired;

    private static ArrayList<String> dynamicallyConfigurableOptions = new ArrayList<String>() {{
        add("broker.id");
        add("broker.rack");
        //add("listeners");
        add("inter.broker.listener.name");
    }};

    public KafkaConfigurationDiff(String current, String desired) {
        this.current = current;
        this.desired = desired;
    }

    public ArrayList<String> getOptionsWhichAreDifferent() {
        Properties aProps = new Properties();
        Properties bProps = new Properties();
        ArrayList<String> result = new ArrayList<String>();

        for (String line: current.split(System.getProperty("line.separator"))) {
            line = line.replaceAll(" ", "");
            line = line.replace(System.getProperty("line.separator"), "");
            if (line.startsWith("#")) {
                continue;
            }
            String[] property = line.split("=");
            if (property.length == 2)
                aProps.put(property[0], property[1]);
            else
                aProps.put(property[0], "");
        }

        for (String line: desired.split(System.getProperty("line.separator"))) {
            line = line.replaceAll(" ", "");
            line = line.replace(System.getProperty("line.separator"), "");
            if (line.startsWith("#")) {
                continue;
            }
            String[] property = line.split("=");
            if (property.length == 2)
                bProps.put(property[0], property[1]);
            else
                bProps.put(property[0], "");
        }

        for (String aKey: aProps.stringPropertyNames()) {
            String aVal = aProps.getProperty(aKey);
            String bVal = bProps.getProperty(aKey);

            if (!aVal.equals(bVal)) {
                result.add(aKey);
            }
        }

        for (String bKey: bProps.stringPropertyNames()) {
            String aVal = aProps.getProperty(bKey);
            String bVal = bProps.getProperty(bKey);

            if ((aVal == null && bVal != null) || (!aVal.equals(bVal) && !result.contains(bKey))) {
                result.add(bKey);
            }
        }
        return result;
    }

    /**
     * @return true whether the diff is empty or only dynamically configurable options are changed
     */
    public boolean isDesiredConfigDynamicallyConfigurable() {
        for (String option: getOptionsWhichAreDifferent()) {
            if (!dynamicallyConfigurableOptions.contains(option)) {
                return false;
            }
        }
        return true;
    }
}
