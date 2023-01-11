using System;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using MySql.Data.MySqlClient;
using EasyModbus;
using System.Data;
using System.Security.Principal;

namespace MB_TO_SQL_Linux
{
    class Program
    {
        public static string IP; //IP Сервера
        public static string user; //Имя пользователя
        public static string pass; //Пароль
        public static string schema; //База
        public static string table_name; //Таблица
        public static string max_var; //Количество синхранизируемых переменных
        public static ConsoleKeyInfo cki; //Отслеживание нажатия клавиши
        public static SQL Base = new SQL(); //Для работы с базой
        public static ModbusServer MBServer = new ModbusServer(); //Для запуска модбас сервера
        public static short[] mirror = new short[MBServer.holdingRegisters.Length]; //Зеркало переменных
        public static SQL.SQL_Data DATA = new SQL.SQL_Data(); //Дата для работы с базой данных
        public static Boolean first_time = true; //Бит первого запуска
        public static string update_rows = ""; //Строка для сохранения номеров участвующий в обновлениее строк
        public static string update_values = ""; //строка для сохранения новых значений
        public static int[] update_intervals;   //Собираем интервалы где произошло обновление переменных
        public static int split_interval;
        public static int MySQL_Data = new int();

        static void Main(string[] args)
        {
            string[] path_split = Assembly.GetExecutingAssembly().Location.Split('/'); //Пробуем разбить путь по слешке (Linux)
            if (path_split.Length<=1) path_split = Assembly.GetExecutingAssembly().Location.Split('\\'); //если не получилось разбить путь пробуем с другой слешкой (Windows)
            string path = Assembly.GetExecutingAssembly().Location.Replace(path_split[path_split.Length-1],""); //убираем название exe файла из пути и получаем путь к папке
            
            using (StreamReader sr = File.OpenText(path+"config.ini")) //Открываем файл конфига для чтения
            {
                try //Пробуем прочитать все конфиги
                {
                    IP = sr.ReadLine().Trim();
                    user = sr.ReadLine().Trim();
                    pass = sr.ReadLine().Trim();
                    schema = sr.ReadLine().Trim();
                    table_name = sr.ReadLine().Trim();
                    max_var = sr.ReadLine().Trim();
                    split_interval = Convert.ToInt32(sr.ReadLine().Trim());
                }
                catch //При возникновении ошибки сообщаем об этом и говорим о прочтении example.ini
                {
                    Console.WriteLine("Config Error! Read example.ini");

                    if (!File.Exists(path+"example.ini")) //генерируем example.ini
                    {
                        // Create a file to write to.
                        using (StreamWriter sw = File.CreateText(path+"example.ini")) 
                        {
                            sw.WriteLine("127.0.0.1");
                            sw.WriteLine("root");
                            sw.WriteLine("root");
                            sw.WriteLine("Wells");
                            sw.WriteLine("Value");
                            sw.WriteLine("3000");
                            sw.WriteLine("50");
                        }
                    }
                    Console.ReadKey();  //Ждем когда пользователь нажмет кнопку
                    Environment.Exit(0); //Закрываем приложение
                }
            }
            Console.WriteLine("Press Q for Exit!"); //Сообщаем что приложение может быть закрыто по Q
            Base.Init(IP, schema, user, pass); //Настраиваем подключение к базе
            //Base.New_Table(table_name);
            MBServer.Listen(); //Запускаем модбас сервер

            while (1 == 1) //бесконечный цикл
            {
                try
                {  
                    if (Console.KeyAvailable == true) //Отслеживаем была ли нажата кнопка 
                    {
                        cki = Console.ReadKey(true); //читаем кнопку
                        if (cki.Key == ConsoleKey.Q) //Если это Q - закрываем программу
                        {
                            Console.WriteLine("Exit!");
                            Environment.Exit(0);
                        }
                    }

                    if (first_time) 
                    {
                        Console.WriteLine("Try To Connect:");
                        Console.WriteLine(IP +":"+schema+"."+table_name);

                        DATA = Base.Get_Table_2_Column(table_name, "ID", table_name, max_var); //Пробуем считать таблицу

                        while ((DATA.ErrorText == "Unable to connect to any of the specified MySQL hosts.")) //Если связи с сервером нет, пытаемся читать таблицу
                        {
                            if (Console.KeyAvailable == true) //Кнопка выхода
                            {
                                cki = Console.ReadKey(true);
                                if (cki.Key == ConsoleKey.Q)
                                {
                                    Console.WriteLine("Exit!");
                                    Environment.Exit(0);
                                }
                            }
                            Console.WriteLine("Reconnect!");
                            DATA = Base.Get_Table_2_Column(table_name, "ID", table_name, max_var);  
                        }

                        if ((!DATA.NoError) & (DATA.ErrorText != "Unable to connect to any of the specified MySQL hosts.")) //Если таблицы нет или она имеет не правильный формат
                        {
                            //MessageBox.Show("Создание базы");
                            Console.WriteLine("Create Table!");
                            Base.Delete_Table(table_name); //Удаляем её
                            Base.New_Table(table_name); //Создаем заного
                            Console.WriteLine("Create Rows!");
                            Base.New_Rows(table_name, Convert.ToInt32(max_var)); //Создаем нужное кол-во строк
                        }
                        else 
                        {
                            if (DATA.Data.Rows.Count < Convert.ToInt32(max_var)) //Если не хватает строк
                            {
                                // MessageBox.Show("Добавление строк");
                                Console.WriteLine("Create Rows!");
                                Base.Select_Increment(table_name, DATA.Data.Rows.Count + 1); //Меняем автоинкремент у таблицы
                                Base.New_Rows(table_name, Convert.ToInt32(max_var)-DATA.Data.Rows.Count); //создаем нехватающие строки

                            }
                        }

                        Console.WriteLine("Connection Done!");
                        Console.WriteLine("Start Transfer!");
                        first_time = false;
                    }

                    //DATA = Base.Get_Table_Begin(table_name, "ID", max_var);
                    DATA = Base.Get_Table_2_Column(table_name, "ID", table_name, max_var); //читаем таблицу
                    
                    if (DATA.NoError) //Если прочиталось без ошибок
                    {
                        update_intervals = new int[DATA.Data.Rows.Count/100];
                        for (int i = 0; i < DATA.Data.Rows.Count; i++) //На всех считанных строках
                        {
                            if (mirror[i] != (Int16)(Convert.ToInt32(DATA.Data.Rows[i][1]))) //Проверяем несовпадение базы с зеркалом
                            {
                                MySQL_Data = (MySQL_Data|1);
                                MBServer.holdingRegisters[i+1] = (Int16)(Convert.ToInt32(DATA.Data.Rows[i][1])); //Заменяем модбас значение
                                mirror[i] = (Int16)(Convert.ToInt32(DATA.Data.Rows[i][1])); //вносим новое в базу
                            }

                            if (mirror[i] != MBServer.holdingRegisters[i + 1]) //проверяем несовпадение модбас с зеркалом
                            {
                                for (int j = 1; j <=DATA.Data.Rows.Count/split_interval; j++) //Делим переменные на промежутки
                                {
                                    if (((i+1)>=(j-1)*split_interval)&((i+1)<=j*split_interval)) //Принадлежит ли промежутку наша переменная
                                    {
                                        update_intervals[j - 1] = j;    //Если принадлежит, запоминаем номер промежутка
                                        j = DATA.Data.Rows.Count / split_interval; //Останавливаем for
                                        
                                    }
                                }
                                update_rows += (i + 1).ToString() + ","; //заносим номер строки изменяемой переменной
                                update_values += MBServer.holdingRegisters[i + 1].ToString() + ","; //заносим новое значение
                                mirror[i] = MBServer.holdingRegisters[i + 1]; //изменяем зеркало
                            }
                        }
                     
                        if (update_rows != "") //Если есть что обновлять
                        {
                            Base.Update_Data(table_name, "ID", table_name, update_rows.Substring(0, update_rows.Length - 1), update_values.Substring(0, update_values.Length - 1)); //Делаем большой запрос на обновление
                            update_rows = ""; //Стираем вспомогательные строки
                            update_values = "";
                            foreach(int interval in update_intervals) 
                            {
                                if (interval > 0)
                                {
                                    Base.Update_Time(table_name, "ID", "Update", (interval-1)* split_interval, interval* split_interval);
                                }
                            }                  
                        }
                    }
                    else //Если произошла ошибка при запросе
                    {
                        Console.WriteLine(DATA.ErrorText.ToString()); //Выводим номер ошибки
                        first_time = true; //Пытаемся переподключиться
                    }

                    //System.Threading.Thread.Sleep(1000);
                }
                catch(Exception e)
                {
                    /*Closed += OnClose;
                    Console.Write("Restart!");
                    Closed(System.Diagnostics.Process.GetCurrentProcess(), null);*/
                    Console.Write("Error: "+e);
                    Console.ReadKey();
                    Environment.Exit(0);

                }
            }
            
        }
    }

    

    class SQL
    {
        private static string ip;
        private static string base_name;
        private static string user_name;
        private static string pass;

        public void set_ip(string new_ip)
        {
            ip = new_ip;
        }

        public void set_base_name(string new_base_name)
        {
            base_name = new_base_name;
        }

        public void set_user_name(string new_user_name)
        {
            user_name = new_user_name;
        }

        public void set_pass(string new_pass)
        {
            pass = new_pass;
        }

        private static MySql.Data.MySqlClient.MySqlConnection Connect_SQL; //= new MySql.Data.MySqlClient.MySqlConnection(connection);
        private static MySql.Data.MySqlClient.MySqlCommand Command_SQL;

        public class SQL_Result
        {
            /// <summary>
            /// Возвращает результат запроса.
            /// </summary>
            public string Result;
            /// <summary>
            /// Возвращает True - если произошла ошибка.
            /// </summary>
            public string ErrorText;
            /// <summary>
            /// Возвращает текст ошибки.
            /// </summary>
            public bool NoError;
        }

        public class SQL_Data
        {
            /// <summary>
            /// Возвращает результат запроса.
            /// </summary>
            public DataTable Data;
            /// <summary>
            /// Возвращает True - если произошла ошибка.
            /// </summary>
            public string ErrorText;
            /// <summary>
            /// Возвращает текст ошибки.
            /// </summary>
            public bool NoError;
        }

        public void Init(string new_ip, string new_base_name, string new_user_name, string new_pass)
        {
            set_ip(new_ip);
            set_base_name(new_base_name);
            set_user_name(new_user_name);
            set_pass(new_pass);
        }

        private static SQL_Result Connect()
        {
            SQL_Result result = new SQL_Result();
            try
            {
                Connect_SQL = new MySql.Data.MySqlClient.MySqlConnection("Database=" + base_name + ";Data Source=" + ip + ";User Id=" + user_name + ";Password=" + pass);
                Connect_SQL.Open();
                result.NoError = true;
            }
            catch (Exception ex) //Этот эксепшн на случай отсутствия соединения с сервером.
            {
                result.NoError = false;
                result.ErrorText = ex.Message;
            }
            return result;
        }

        private static void Disconnect()
        {
            try
            {
                Connect_SQL.Close();
            }
            catch
            {

            }
        }

        public static SQL_Result SqlNoneQuery(string sql)
        {
            SQL_Result result = new SQL_Result();

            if (Connect().NoError)
            {
                Command_SQL = new MySql.Data.MySqlClient.MySqlCommand(sql, Connect_SQL);

                try
                {
                    Command_SQL.ExecuteNonQuery();
                    result.NoError = true;
                }
                catch (Exception ex)
                {
                    result.NoError = false;
                    result.ErrorText = ex.Message;
                }

                Disconnect();
            }
            else
            {
                result.NoError = false;
                result.ErrorText = Connect().ErrorText;
            }
            return result;
        }

        public static SQL_Data SqlData(string sql)
        {
            SQL_Data result = new SQL_Data();

            if (Connect().NoError)
            {
                Command_SQL = new MySql.Data.MySqlClient.MySqlCommand(sql, Connect_SQL);

                try
                {
                    MySql.Data.MySqlClient.MySqlDataAdapter Adapter_SQL = new MySql.Data.MySqlClient.MySqlDataAdapter();
                    Adapter_SQL.SelectCommand = Command_SQL;
                    DataSet DataSet_SQL = new DataSet();
                    Adapter_SQL.Fill(DataSet_SQL);
                    result.Data = DataSet_SQL.Tables[0];
                    result.NoError = true;
                }
                catch (Exception ex)
                {
                    result.NoError = false;
                    result.ErrorText = ex.Message;
                }

                Disconnect();
            }
            else
            {
                result.NoError = false;
                result.ErrorText = Connect().ErrorText;
            }
            return result;
        }



        public SQL_Data Get_Max_Where(string need_column, string table_name, int min, int max, string sort_column)  //Найти значение в колонке по максимальному значению в другой
        {
            return SqlData("SELECT " + need_column + " FROM " + base_name + "." + table_name + " WHERE "+need_column + ">="+ min.ToString() + " AND " + need_column+"<="+max.ToString() + " ORDER BY `"+
                sort_column + "` DESC LIMIT 1");
        }

        public SQL_Result Update_Time(string table_name, string id_column, string time_column, int min, int max)    //Обновить время у промежутка пееменных
        {
            string id_max_time = this.Get_Max_Where(id_column, table_name, min, max, time_column).Data.Rows[0][0].ToString();
            return SqlNoneQuery("UPDATE "+base_name+"."+table_name+" AS a INNER JOIN "+base_name+"."+table_name+" AS b ON b.ID = "+id_max_time+" SET a.`"+time_column+"` = b.`"+time_column
                +"` WHERE a.ID >="+min.ToString()+" AND a.ID<="+max.ToString());
        }

        public SQL_Data Get_Table(string table_name)
        {

            return SqlData("select * from " + base_name + "." + table_name);
        }

        public SQL_Data Get_Table(string table_name, string column_sort_name)
        {

            return SqlData("select * from " + base_name + "." + table_name + " ORDER BY " + column_sort_name);
        }

        public SQL_Data Get_Table_How(string table_name, string begin, string how)
        {

            return SqlData("select * from " + base_name + "." + table_name + " LIMIT " + begin + "," + how);
        }

        public SQL_Data Get_Table_How(string table_name, string begin, string how, string column_sort_name)
        {

            return SqlData("select * from " + base_name + "." + table_name + " ORDER BY " + column_sort_name + " LIMIT " + begin + "," + how);
        }


        public SQL_Data Get_Table_End(string table_name, string column_sort_name, string how)
        {
            return SqlData("select * from " + base_name + "." + table_name + " ORDER BY " + column_sort_name + " DESC LIMIT " + how);
        }

        public SQL_Data Get_Table_Begin(string table_name, string column_sort_name, string how)
        {
            return SqlData("select * from " + base_name + "." + table_name + " ORDER BY " + column_sort_name + " LIMIT " + how);
        }

        public SQL_Data Get_Table_2_Column(string table_name, string column_sort_name, string second_column_name, string how)
        {
            return SqlData("select " + column_sort_name + ", " + second_column_name + " from " + base_name + "." + table_name + " ORDER BY " + column_sort_name + " LIMIT " + how);
        }

        public SQL_Result SQL_Command(string comm)
        {
            return SqlNoneQuery(comm);
        }

        public string SQL_Command_Set(string table_name, string rows, string values, string column_condition, string condition)
        {
            string[] row_split;
            string[] value_split;

            row_split = rows.Split(',');
            value_split = values.Split(',');

            string comm = "UPDATE `" + base_name + "`.`" + table_name + "` SET ";

            for (int i = 0; i < row_split.Length; i++)
            {
                if (i < value_split.Length)
                {
                    comm += "`" + row_split[i] + "`='" + value_split[i] + "'";
                }
                else
                {
                    comm += "`" + row_split[i] + "`=''";
                }
                if ((i + 1) < row_split.Length)
                {
                    comm += ',';
                }
            }

            comm += " WHERE `" + column_condition + "`='" + condition + "'; ";

            /*Base.Set_Value(table_name, "Value", MBServer.holdingRegisters[i + 1].ToString(), "ID", (i + 1).ToString());*/
            return comm;
        }
        public SQL_Result Update_Data(string table_name, string column_condition_name, string column_value_name, string rows, string values)
        {
            string[] row_split;
            string[] value_split;

            row_split = rows.Split(',');
            value_split = values.Split(',');

            string comm = "INSERT INTO `" + base_name + "`.`" + table_name + "` ("+column_condition_name+","+column_value_name+") VALUES ";

            for (int i = 0; i < row_split.Length; i++)
            {
                comm +="("+row_split[i]+","+value_split[i]+")";
                if (i < row_split.Length-1) {
                    comm += ",";
                }
            }

            comm += " ON DUPLICATE KEY UPDATE "+column_value_name+"= VALUES ("+column_value_name+");";
            return SqlNoneQuery(comm);
        }
        public SQL_Result Set_Value(string table_name, string rows, string values, string column_condition, string condition)
        {
            string[] row_split;
            string[] value_split;

            row_split = rows.Split(',');
            value_split = values.Split(',');

            string comm = "UPDATE `" + base_name + "`.`" + table_name + "` SET ";

            for (int i = 0; i < row_split.Length; i++)
            {
                if (i < value_split.Length)
                {
                    comm += "`" + row_split[i] + "`='" + value_split[i] + "'";
                }
                else
                {
                    comm += "`" + row_split[i] + "`=''";
                }
                if ((i + 1) < row_split.Length)
                {
                    comm += ',';
                }
            }

            comm += " WHERE `" + column_condition + "`='" + condition + "';";

            /*Base.Set_Value(table_name, "Value", MBServer.holdingRegisters[i + 1].ToString(), "ID", (i + 1).ToString());*/
            return SqlNoneQuery(comm);
        }

        public SQL_Result New_Row(string table_name)
        {
            string comm = "INSERT INTO `" + base_name + "`.`" + table_name + "` () VALUE();";
            return SqlNoneQuery(comm);
        }

        public SQL_Result New_Rows(string table_name, int how)
        {
            string comm = "INSERT INTO `" + base_name + "`.`" + table_name + "` () VALUES "; 
            for (int i = 0; i < how; i++)
            {
                comm += "()";
                if (i < how - 1)
                {
                    comm += ",";
                }
            }
            comm += ";";
            return SqlNoneQuery(comm);
        }

        public SQL_Result New_Table(string table_name)
        {
            string comm = "CREATE TABLE " + base_name + "." + table_name + " (`ID` INT NOT NULL AUTO_INCREMENT, `Value` INT NOT NULL DEFAULT '0', `Update` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`ID`));";
            return SqlNoneQuery(comm);
        }
        public SQL_Result Delete_Row(string table_name, string column_condition, string condition)
        {
            string comm = "DELETE FROM `" + base_name + "`.`" + table_name + "` WHERE `" + column_condition + "`='" + condition + "';";

            return SqlNoneQuery(comm);
        }

        public SQL_Result Clear_Table(string table_name)
        {
            string comm = "DELETE FROM `" + base_name + "`.`" + table_name + "`;";

            return SqlNoneQuery(comm);
        }

        public SQL_Result Delete_Table(string table_name)
        {
            string comm = "DROP TABLE `" + base_name + "`.`" + table_name + "`;";

            return SqlNoneQuery(comm);
        }

        public SQL_Result Select_Increment(string table_name, int Increment)
        {
            string comm = "ALTER TABLE `" + base_name + "`.`" + table_name + "` AUTO_INCREMENT = " + Convert.ToString(Increment) + " ;";

            return SqlNoneQuery(comm);
        }
    }

  /*  class IniFile
    {
        string Path; //Имя файла.

        [DllImport("kernel32")] // Подключаем kernel32.dll и описываем его функцию WritePrivateProfilesString
        static extern long WritePrivateProfileString(string Section, string Key, string Value, string FilePath);

        [DllImport("kernel32")] // Еще раз подключаем kernel32.dll, а теперь описываем функцию GetPrivateProfileString
        static extern int GetPrivateProfileString(string Section, string Key, string Default, StringBuilder RetVal, int Size, string FilePath);

        // С помощью конструктора записываем пусть до файла и его имя.
        public IniFile(string IniPath)
        {
            Path = new FileInfo(IniPath).FullName.ToString();
        }

        //Читаем ini-файл и возвращаем значение указного ключа из заданной секции.
        public string ReadINI(string Section, string Key)
        {
            var RetVal = new StringBuilder(255);
            GetPrivateProfileString(Section, Key, "", RetVal, 255, Path);
            return RetVal.ToString();
        }
        //Записываем в ini-файл. Запись происходит в выбранную секцию в выбранный ключ.
        public void Write(string Section, string Key, string Value)
        {
            WritePrivateProfileString(Section, Key, Value, Path);
        }

        //Удаляем ключ из выбранной секции.
        public void DeleteKey(string Key, string Section = null)
        {
            Write(Section, Key, null);
        }
        //Удаляем выбранную секцию
        public void DeleteSection(string Section = null)
        {
            Write(Section, null, null);
        }
        //Проверяем, есть ли такой ключ, в этой секции
        public bool KeyExists(string Section, string Key)
        {
            return ReadINI(Section, Key).Length > 0;
        }
    }*/
}
