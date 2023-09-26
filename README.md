# Gıda Fiyatları İzleme Projesi

Bu proje, gıda fiyatlarının değişimini izlemek için tasarlanmıştır. Airflow, Python, PostgreSQL ve Docker teknolojilerini kullanarak, gündelik olarak hedef ürünlerin fiyat bilgilerinin toplanması ve uygun formatta PostgreSQL veritabanına kaydedilmesi işlemini gerçekleştirir.

## Projenin Amacı

Gıda fiyatlarının zaman içindeki değişimini gözlemlemek.

## Projenin Kapsamı

* Airflow ile web scraping kodları oluşturma
* Verileri uygun formata dönüştürme
* Verileri PostgreSQL veritabanına kaydetme

## Projenin Kullanılan Teknolojiler

* Airflow
* Python
* PostgreSQL
* Docker

## Projenin Kurulumu

1. Docker'ı kurun.
2. Projeyi indirin.
3. `docker-compose up -d` komutunu çalıştırın.

## Projenin Kullanımı

Proje, arka planda sürekli olarak çalışmaktadır. Veriler, her gün saat airflow dagları üzerinde belirtilen saatlerde PostgreSQL veritabanına kaydedilir.

## Veriler

DataSet klasörü içerisinde bulunan LastYearProductPriceByDaily.json adlı dosyada 01-08-2022 <-> 01-08-2023 tarihleri arasındaki belirlenen ürünlerin örnek bir datası bulunmaktadır.

## Geliştirme

Proje, açık kaynak olarak geliştirilmektedir. Katkılarınızı bekliyoruz.

## Teknik Detaylar

Proje, aşağıdaki adımları izleyerek çalışır:

1. Airflow, web scraping kodlarını çalıştırarak hedef ürünlerin fiyat bilgilerini toplar.
2. Toplanan veriler, uygun formata dönüştürülür.
3. Veriler, PostgreSQL veritabanına kaydedilir.

Proje, Docker container'ları kullanarak oluşturulmuştur. Bu sayede, projenin farklı ortamlarda kolayca kurulup çalıştırılması sağlanmıştır.

## Geliştirme Planları

Gelecekte, proje aşağıdaki geliştirmeleri içerecek şekilde genişletilecektir:

* Daha fazla ürün için fiyat izleme desteği
* Verilerin daha ayrıntılı analizini sağlayan araçlar
* Projenin daha kolay kullanılmasını sağlayan kullanıcı arayüzü

## Katkı Yönergeleri

Projeye katkıda bulunmak için aşağıdaki adımları takip edebilirsiniz:

1. Projeyi forklayın.
2. Değişiklikleri yapın.
3. Değişiklikleri commit edin ve pull request gönderin.

Katkılarınız için teşekkür ederiz!
