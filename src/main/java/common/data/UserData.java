package common.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class UserData implements Serializable {
    private Map<String, Object> data = new HashMap<>();

    public UserData()
    {
    }

    public void add(String name, Object value)
    {
        data.put(name, value);
    }

    public void remove(String name)
    {
        data.remove(name);
    }

    public int getValueInt(String name)
    {
        return Integer.parseInt(data.get(name).toString());
    }

    public boolean getValueBool(String name)
    {
        return Boolean.parseBoolean(data.get(name).toString());
    }

    public double getValueDouble(String name)
    {
        return Double.parseDouble(data.get(name).toString());
    }
}
