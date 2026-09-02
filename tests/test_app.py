import json
import base64
import tempfile
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from unittest import mock

from PIL import Image

import app


def test_image(marker=b""):
    output = BytesIO()
    image = Image.effect_noise((180, 120), 80).convert("RGB")
    offset = sum(marker) % 120
    for x in range(offset, min(offset + 25, image.width)):
        for y in range(20, 60):
            image.putpixel((x, y), (255, 0, 0))
    image.save(output, format="JPEG", quality=90)
    return output.getvalue()


TEST_IMAGE = test_image()
CERTIFIED_IMAGE = test_image(b"certified-final")


class FakeResponse:
    def __init__(self, content=b"", payload=None, url="https://images.example/photo.jpg", content_type="image/jpeg", status_code=200):
        self.content = content if content.startswith((b"\xff\xd8", b"\x89PNG")) else test_image(content)
        self._payload = payload
        self.url = url
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class AppTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.originals = {
            "DATA_ROOT": app.DATA_ROOT,
            "PACKAGES_ROOT": app.PACKAGES_ROOT,
            "DB_PATH": app.DB_PATH,
            "LOGO_PATH": app.LOGO_PATH,
            "LOGO_URL": app.LOGO_URL,
            "PUBLIC_BASE_URL": app.PUBLIC_BASE_URL,
            "AI_ENDPOINT": app.AI_ENDPOINT,
            "AI_PACKAGES_ROOT": app.AI_PACKAGES_ROOT,
            "AI_MODE": app.AI_MODE,
            "AI_TOKEN": app.AI_TOKEN,
            "AI_BRIDGE_ENABLED": app.AI_BRIDGE_ENABLED,
            "SOURCE_LISTINGS_ROOT": app.SOURCE_LISTINGS_ROOT,
            "AI_REQUIRED": app.AI_REQUIRED,
            "MEDIA_GITHUB_REPO": app.MEDIA_GITHUB_REPO,
            "MEDIA_GITHUB_BRANCH": app.MEDIA_GITHUB_BRANCH,
            "GITHUB_TOKEN": app.GITHUB_TOKEN,
            "SHEETS_WEBHOOK_URL": app.SHEETS_WEBHOOK_URL,
            "SHEETS_WEBHOOK_SECRET": app.SHEETS_WEBHOOK_SECRET,
            "SHEETS_OUTBOX_ROOT": app.SHEETS_OUTBOX_ROOT,
        }
        app.DATA_ROOT = root / "data"
        app.PACKAGES_ROOT = app.DATA_ROOT / "packages"
        app.DB_PATH = app.DATA_ROOT / "block3.sqlite3"
        app.LOGO_PATH = root / "logo.jpg"
        app.LOGO_PATH.write_bytes(b"logo" * 1024)
        app.LOGO_URL = ""
        app.PUBLIC_BASE_URL = ""
        app.AI_ENDPOINT = ""
        app.AI_MODE = "browser"
        app.AI_TOKEN = ""
        app.AI_BRIDGE_ENABLED = False
        app.SOURCE_LISTINGS_ROOT = None
        app.AI_REQUIRED = False
        app.MEDIA_GITHUB_REPO = ""
        app.MEDIA_GITHUB_BRANCH = "media"
        app.GITHUB_TOKEN = ""
        app.SHEETS_WEBHOOK_URL = ""
        app.SHEETS_WEBHOOK_SECRET = ""
        app.SHEETS_OUTBOX_ROOT = app.DATA_ROOT / "sheets_outbox"
        app.init_storage()

    def tearDown(self):
        for name, value in self.originals.items():
            setattr(app, name, value)
        self.temp.cleanup()

    def payload(self):
        return {
            "internal_id": "203781",
            "source": "https://rieltor.ua/flats-sale/view/203781/",
            "translations": {
                "uk": {"title": "Квартира", "text": "Світла квартира з ремонтом."},
                "en": {"title": "Apartment", "text": "A bright renovated apartment."},
            },
            "details": {"price": "97000", "currency": "USD", "area": "97", "floor": "7", "total_floors": "18", "rooms": "3", "property_type": "apartment"},
            "prices": {"UAH": "4000000", "USD": "97000", "EUR": "89000"},
            "images": ["https://images.example/photo.jpg"],
        }

    def test_public_text_removes_source_and_agent_sentences(self):
        text = "Світла квартира. Телефонуйте рієлтору. Комісія 3%."
        self.assertEqual(app.sanitize_public_text(text), "Світла квартира.")

    def test_rieltor_gallery_rejects_icons_avatars_and_site_art(self):
        images = [
            "https://rieltor.ua/img/menu/icon_menu_flats.svg",
            "https://rieltor-images.lunstatic.net/rieltor-ua-01/120/120/avatars/1.jpg",
            "https://market-images.lunstatic.net/lun-ua/310/310/images/offers/preview.jpg",
            "https://market-images.lunstatic.net/lun-ua/t.1.0.0/1600/1200/images/offers/room.jpg",
            "https://rieltor-images.lunstatic.net/rieltor-ua-01/1920/1440/offers/full-size.jpg",
            "https://rieltor.ua/img/mastercard.svg",
        ]
        self.assertEqual(
            app.listing_photo_urls(images, "https://rieltor.ua/flats-rent/view/12883087/"),
            [images[3], images[4]],
        )

    def test_title_rejects_rieltor_resource_tail(self):
        self.assertEqual(
            app.sanitize_title("Apartment - RIELTOR.UAResource 1Resource 1"),
            "Apartment",
        )
        self.assertEqual(app.sanitize_title("Apartment RIELTOR.UA"), "Apartment")

    def test_title_removes_advertisement_number_prefix(self):
        self.assertEqual(app.sanitize_title("Advertisement # 4512: Apartment in Kyiv"), "Apartment in Kyiv")

    def test_title_and_ai_text_remove_listing_number_and_are_editorially_distinct(self):
        title = "Продаж квартир: Мечникова вул., 11-А - Оголошення №12519316"
        source = "Оголошення №12519316. Світла квартира з ремонтом поруч із центром."
        ai_text = app.editorial_ai_text(source, title)
        self.assertNotIn("12519316", app.sanitize_title(title))
        self.assertNotIn("12519316", ai_text)
        self.assertIn("Представляємо ретельно відібрану пропозицію", ai_text)
        self.assertNotEqual(app.sanitize_public_text(source), ai_text)

    def test_property_details_follow_apartment_commercial_and_house_templates(self):
        apartment = app.extract_details("Продаж 2-кімнатної квартири. 450 000 $ 70.30 м² Поверх 1 з 5")
        commercial = app.extract_details("Оренда теплого складу класу B+. 200 грн 1 000 м² Поверх 1")
        house = app.extract_details("Сучасний 3-х поверховий будинок. 3 999 $ 500 м² 7 кімнат Ділянка 10 соток")
        self.assertEqual(apartment["property_type"], "apartment")
        self.assertEqual(app.property_detail_rows(apartment, "uk"), [("Загальна площа", "70.30 м²"), ("Поверх", "1/5"), ("Кількість кімнат", "2 кімнати")])
        self.assertEqual(commercial["property_type"], "commercial")
        self.assertEqual(app.property_detail_rows(commercial, "uk"), [("Загальна площа", "1 000 м²"), ("Поверх", "1")])
        self.assertEqual(house["property_type"], "house")
        self.assertEqual(app.property_detail_rows(house, "uk"), [("Загальна площа", "500 м²"), ("Кількість кімнат", "7 кімнати"), ("Площа ділянки", "10 соток"), ("Поверховість", "3")])

    def test_house_plural_title_and_storeys_field_are_rendered_as_one_value(self):
        house = app.extract_details("Продаж будинків. Площа 500 м². Поверхів: 3", "Продаж будинків")
        self.assertEqual(house["property_type"], "house")
        self.assertEqual(house["total_floors"], "3")
        self.assertEqual(app.property_detail_rows(house, "uk"), [("Загальна площа", "500 м²"), ("Поверховість", "3")])

    def test_house_land_area_recognizes_plot_variants(self):
        for source in ("Будинок. Площа ділянки: 10 соток", "Будинок. Земельна ділянка 10 сот.", "Будинок. Участок: 0.1 га"):
            details = app.extract_details(source, "Продаж будинків")
            self.assertTrue(details["land_area"])
        self.assertEqual(app.extract_details("Будинок. Площа ділянки: 10 сот.", "Продаж будинків")["land_area"], "10 соток")

    def test_house_land_area_can_come_from_full_offer_description(self):
        title = "Продаж будинків: Голосіївський район, Київ - Оголошення №77"
        full_description = "Сучасний будинок. Площа ділянки: 10 соток."
        details = app.extract_details(full_description, title)
        self.assertEqual(details["property_type"], "house")
        self.assertEqual(details["land_area"], "10 соток")

    def test_house_land_area_recognizes_compact_fact_row_without_label(self):
        details = app.extract_details("Будинок 839 м² на березі Дніпра. 839 м² | 36 соток | власний вихід до затоки.", "Продаж будинків")
        self.assertEqual(details["land_area"], "36 соток")

    def test_rieltor_uses_clean_title_address_and_area_is_not_a_room_count(self):
        title = "Продаж квартир: Мечникова вул. (Кловський), 11-А, Печерський р-н, Київ - Оголошення №12519316"
        address = app.extract_address("Київ Печерський р-н Продаж квартир", "https://rieltor.ua/flats-sale/view/12519316/", title)
        self.assertEqual(address, "Мечникова вул. (Кловський), 11-А, Печерський р-н, Київ")
        details = app.extract_details("Продаж квартир. Площа 165 м². Поверх 5 з 11", title)
        self.assertEqual(details["property_type"], "apartment")
        self.assertEqual(details["rooms"], "")

    def test_explicit_room_count_wins_over_room_area(self):
        text = "Кімната 12 кв. м. Спальні 19 і 13 кв. м. Планування. Кількість кімнат: 4. Загальна площа: 149 м²."
        self.assertEqual(app.extract_details(text, "Продаж будинку")["rooms"], "4")

    def test_cleaned_title_does_not_replace_rieltor_address_source(self):
        raw_title = "Продаж квартир: Мечникова вул. (Кловський), 11-А, Печерський р-н, Київ - Оголошення №12519316"
        cleaned_title = app.sanitize_title(raw_title)
        self.assertEqual(cleaned_title, "Продаж квартир: Мечникова вул. (Кловський), 11-А, Печерський р-н, Київ")
        self.assertEqual(app.extract_address("Київ Печерський р-н Продаж квартир", "https://rieltor.ua/flats-sale/view/12519316/", raw_title), "Мечникова вул. (Кловський), 11-А, Печерський р-н, Київ")

    def test_english_property_rows_use_translated_address(self):
        details = {"property_type": "apartment", "floor": "4", "total_floors": "12", "address": "Київ, вул. Мечникова, 11-А", "address_en": "Kyiv, Mechnykova Street, 11-A"}
        self.assertIn(("Address", "Kyiv, Mechnykova Street, 11-A"), app.property_detail_rows(details, "en"))
        self.assertIn(("Floor", "4/12"), app.property_detail_rows(details, "en"))

    def test_telegraph_uses_clean_source_price_and_property_rows(self):
        content = app.telegraph_content(self.payload(), "uk", "Чистий опис.")
        text = json.dumps(content, ensure_ascii=False)
        self.assertIn("Ціна: ", text); self.assertIn("97 000 $", text)
        self.assertIn("Загальна площа: ", text); self.assertIn("97 м²", text)
        self.assertIn("Поверх: ", text); self.assertIn("7", text)
        self.assertIn("Поверх: ", text); self.assertIn("7/18", text)
        self.assertNotIn("💰", text)

    def test_editor_keeps_both_description_versions_editable_and_moves_language_switch(self):
        interface = (app.ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn('<textarea id="originalText">', interface)
        self.assertIn('<textarea id="text">', interface)
        self.assertIn("originalTranslations", interface)
        self.assertIn("Download PDF · Ukrainian", interface)
        self.assertIn("Download PDF · English", interface)
        self.assertIn("Download all photos", interface)
        self.assertGreater(interface.index('id="topUk"'), interface.index('id="extract"'))

    def test_initial_ai_request_does_not_look_like_all_photos_unchecked(self):
        interface = (app.ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn("if(!mediaPairs.length)return null", interface)

    @mock.patch.object(app, "safe_remote_url", return_value=True)
    @mock.patch.object(app.requests, "get")
    def test_package_persists_original_final_logo_and_manifest(self, get, _safe):
        get.return_value = FakeResponse(content=b"image" * 1024)
        payload = self.payload()
        payload["processing_mode"] = "ai"
        payload["media_choices"] = None
        result = app.create_package(payload)
        package = app.PACKAGES_ROOT / "203781"
        manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
        html = (package / "uk.html").read_text(encoding="utf-8")
        self.assertEqual(result["photo_count"], 1)
        self.assertEqual(result["processed"], ["/packages/203781/photos/01.jpg"])
        self.assertEqual(result["originals"], ["/packages/203781/originals/01.jpg"])
        self.assertTrue((app.DATA_ROOT / "listings/203781/original/01.jpg").is_file())
        self.assertTrue((app.DATA_ROOT / "listings/203781/final/01.jpg").is_file())
        self.assertTrue((package / "assets/kyiv-estate-logo.jpg").is_file())
        self.assertLess(html.index("photos/01.jpg"), html.index("assets/kyiv-estate-logo.jpg"))
        self.assertEqual(manifest["ai_processing"]["result"], "original_verified")

    @mock.patch.object(app, "safe_remote_url", return_value=True)
    @mock.patch.object(app.requests, "get")
    def test_ai_package_keeps_only_checked_photos_in_user_order(self, get, _safe):
        get.side_effect = [
            FakeResponse(content=b"first" * 1024, url="https://images.example/first.jpg"),
            FakeResponse(content=b"second" * 1024, url="https://images.example/second.jpg"),
        ]
        payload = self.payload()
        payload["images"] = ["https://images.example/first.jpg", "https://images.example/second.jpg"]
        payload["processing_mode"] = "ai"
        payload["media_choices"] = [{"order": 2, "kind": "processed"}]
        app.create_package(payload)
        manifest = json.loads((app.PACKAGES_ROOT / "203781/manifest.json").read_text(encoding="utf-8"))
        self.assertEqual([item["order"] for item in manifest["photos"]], [2])
        self.assertEqual([path.name for path in (app.PACKAGES_ROOT / "203781/photos").iterdir()], ["02.jpg"])

    @mock.patch.object(app, "safe_remote_url", return_value=True)
    @mock.patch.object(app.requests, "get")
    def test_ai_package_rejects_when_all_photos_are_unchecked(self, get, _safe):
        get.return_value = FakeResponse(content=b"image" * 1024)
        payload = self.payload()
        payload["processing_mode"] = "ai"
        payload["media_choices"] = []
        with self.assertRaisesRegex(ValueError, "Жодну фотографію"):
            app.create_package(payload)

    def test_telegraph_content_places_logo_after_primary_photo(self):
        content = app.telegraph_content(
            self.payload(), "uk", "Опис квартири.",
            ["https://telegra.ph/file/main.jpg", "https://telegra.ph/file/second.jpg"],
            "https://telegra.ph/file/logo.jpg",
        )
        images = [node["attrs"]["src"] for node in content if node.get("tag") == "img"]
        self.assertEqual(images[:2], ["https://telegra.ph/file/main.jpg", "https://telegra.ph/file/logo.jpg"])
        logo_index = next(index for index, node in enumerate(content) if node.get("tag") == "img" and node["attrs"]["src"].endswith("logo.jpg"))
        phone_index = next(index for index, node in enumerate(content) if node.get("tag") == "p" and any(isinstance(child, dict) and child.get("children") == [app.CONTACT_PHONE] and str(child.get("attrs", {}).get("href", "")).startswith("https://") for child in node.get("children", [])))
        price_index = next(index for index, node in enumerate(content) if node.get("tag") == "h3")
        self.assertLess(logo_index, phone_index)
        self.assertLess(phone_index, price_index)
        hrefs = [child.get("attrs", {}).get("href") for node in content for child in node.get("children", []) if isinstance(child, dict) and child.get("tag") == "a"]
        self.assertIn("https://kyiv.estate/", hrefs)
        self.assertIn("https://t.me/Real_Estate_Agency_premium", hrefs)
        self.assertEqual(content[-1]["children"][0]["children"], ["🏛 Kyiv.Estate — Агентство нерухомості №1 в Києві."])

    @mock.patch.object(app, "github_media_images")
    def test_durable_media_preserves_photo_and_logo_order(self, upload):
        app.MEDIA_GITHUB_REPO = "realestateplatformapi-lang/listing-telegraph"
        app.GITHUB_TOKEN = "secret"
        photo = app.DATA_ROOT / "photo.jpg"
        logo = app.DATA_ROOT / "logo.jpg"
        photo.parent.mkdir(parents=True, exist_ok=True)
        photo.write_bytes(b"photo" * 1024)
        logo.write_bytes(b"logo" * 1024)
        upload.return_value = ["https://raw.example/photo.jpg", "https://raw.example/logo.jpg"]
        self.assertEqual(app.durable_image_urls([photo, logo], "203781"), upload.return_value)
        upload.assert_called_once_with([photo, logo], "203781")

    @mock.patch.object(app.requests, "post")
    def test_telegraph_upload_is_cached_by_hash(self, post):
        post.return_value = FakeResponse(payload=[{"src": "/file/stable.jpg"}])
        image = app.DATA_ROOT / "final.jpg"
        image.parent.mkdir(parents=True, exist_ok=True)
        image.write_bytes(TEST_IMAGE)
        first = app.telegraph_image(image)
        second = app.telegraph_image(image)
        self.assertEqual(first, "https://telegra.ph/file/stable.jpg")
        self.assertEqual(first, second)
        self.assertEqual(post.call_count, 1)

    def test_publish_uses_direct_urls_and_is_idempotent(self):
        payload = self.payload()
        with mock.patch.object(app, "safe_remote_url", return_value=True), \
             mock.patch.object(app, "sync_sheet_record"), \
             mock.patch.object(app, "publish_page", side_effect=["https://telegra.ph/ua-page", "https://telegra.ph/en-page"]) as publish, \
             mock.patch.object(app, "edit_page", side_effect=lambda page_url, _title, _content: page_url) as edit:
            urls = app.publish_bilingual(payload)
            self.assertEqual(urls, {"uk": "https://telegra.ph/ua-page", "en": "https://telegra.ph/en-page"})
            self.assertEqual(publish.call_count, 2)
            self.assertEqual(edit.call_count, 1)
            first_content = publish.call_args_list[0].args[1]
            self.assertIn("https://images.example/photo.jpg", json.dumps(first_content))
            stable_job_id = app.listing_id(payload["source"])
            self.assertFalse((app.PACKAGES_ROOT / stable_job_id).exists())

            second = app.publish_bilingual(payload)
            self.assertEqual(second, urls)
            self.assertEqual(publish.call_count, 2)
            self.assertEqual(edit.call_count, 1)

            payload["images"].append("https://images.example/second.jpg")
            changed = app.publish_single_language(payload, "uk")
            self.assertFalse(changed["unchanged"])
            self.assertEqual(publish.call_count, 2)
            self.assertEqual(edit.call_count, 3)

    def test_publish_adopts_legacy_manifest_page_instead_of_creating_duplicate(self):
        payload = self.payload()
        legacy = app.PACKAGES_ROOT / "old-url-hash"
        legacy.mkdir(parents=True)
        (legacy / "manifest.json").write_text(json.dumps({
            "source": payload["source"] + "?utm_source=legacy",
            "telegraph": {"uk": "https://telegra.ph/existing-ua"},
        }), encoding="utf-8")
        with mock.patch.object(app, "safe_remote_url", return_value=True), \
             mock.patch.object(app, "sync_sheet_record"), \
             mock.patch.object(app, "publish_page") as publish, \
             mock.patch.object(app, "edit_page", return_value="https://telegra.ph/existing-ua") as edit:
            result = app.publish_single_language(payload, "uk")
        self.assertEqual(result["url"], "https://telegra.ph/existing-ua")
        publish.assert_not_called()
        edit.assert_called_once()

    def test_concurrent_publish_creates_only_one_page(self):
        payloads = [json.loads(json.dumps(self.payload())) for _ in range(2)]
        with mock.patch.object(app, "safe_remote_url", return_value=True), \
             mock.patch.object(app, "sync_sheet_record"), \
             mock.patch.object(app, "publish_page", return_value="https://telegra.ph/one-page") as publish, \
             ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda item: app.publish_single_language(item, "uk"), payloads))
        self.assertEqual([item["url"] for item in results], ["https://telegra.ph/one-page"] * 2)
        self.assertEqual(publish.call_count, 1)

    def test_canonical_identity_ignores_tracking_and_photo_list_has_no_count_limit(self):
        base = "https://www.olx.ua/d/uk/obyavlenie/example-IDAbC123.html"
        tracked = base + "?utm_source=test#gallery"
        self.assertEqual(app.external_listing_id(base), "olx:idabc123")
        self.assertEqual(app.listing_id(base), app.listing_id(tracked))
        urls = [f"https://images.example/{index}.jpg?width=1600" for index in range(135)]
        with mock.patch.object(app, "safe_remote_url", return_value=True):
            self.assertEqual(len(app.clean_image_urls(urls)), 135)

    def test_data_normalization_and_public_text_guards(self):
        details = app.extract_details('Продаж будинку у ЖК «River Park». 200 000 $ 1 000 м². 7 кімнат. Поверхів: 3')
        self.assertEqual(details["area"], "1000")
        self.assertEqual(details["rooms"], "7")
        self.assertEqual(details["residential_complex"], "River Park")
        self.assertEqual(app.extract_details("Житловий комплекс поруч. Квартира 70 м²")["residential_complex"], "")
        cleaned = app.sanitize_public_text("Гарний об'єкт. Оригінал оголошення: OLX. Оновлено: сьогодні. Торг можливий.")
        self.assertEqual(cleaned, "Гарний об'єкт.")

    def test_commercial_price_per_square_meter_is_not_presented_as_total(self):
        details = app.extract_details("Оренда офісів. Загальна площа 10 000 м². Оренда від 265 грн/кв.м.", "Оренда комерції")
        self.assertEqual(details["price"], "265")
        self.assertEqual(details["price_per_m2"], "265")
        self.assertEqual(app.display_price(details, {"UAH": "265", "USD": "6", "EUR": "5"}), "265 грн/м² • 6 $/м² • 5 €/м²")
        house = app.extract_details("Будинок 149 м². Ціна 169 000 $. Вартість 1 134 $/м².", "Продаж будинку")
        self.assertEqual(house["price"], "169000")
        self.assertEqual(house["price_per_m2"], "1134")
        self.assertEqual(app.display_price(house, {"UAH": "7500000", "USD": "169000", "EUR": "145000"}), "7 500 000 грн • 169 000 $ • 145 000 €")

    def test_photo_zip_is_in_memory_and_keeps_all_unique_urls(self):
        payload = self.payload()
        payload["images"] = ["https://images.example/one.jpg", "https://images.example/two.jpg"]
        with mock.patch.object(app, "safe_remote_url", return_value=True), mock.patch.object(app.requests, "get") as get:
            get.side_effect = [FakeResponse(content=b"one"), FakeResponse(content=b"two")]
            body = app.make_photo_zip(payload)
        with zipfile.ZipFile(BytesIO(body)) as archive:
            self.assertEqual(archive.namelist(), ["001.jpg", "002.jpg"])
        self.assertFalse(any(path.is_file() for path in app.PACKAGES_ROOT.rglob("*.jpg")))

    def test_sheet_outbox_uses_external_identity_and_does_not_persist_secret(self):
        app.SHEETS_WEBHOOK_SECRET = "do-not-store-this-secret"
        payload = self.payload()
        with mock.patch.object(app, "safe_remote_url", return_value=True):
            app.sync_sheet_record(payload, "telegraph")
        stable_job_id = app.listing_id(payload["source"])
        outbox = app.SHEETS_OUTBOX_ROOT / f"{stable_job_id}.json"
        raw = outbox.read_text(encoding="utf-8")
        envelope = json.loads(raw)
        self.assertEqual(envelope["key"], "rieltor:203781")
        self.assertEqual(envelope["row"]["external_id"], "rieltor:203781")
        self.assertNotIn("do-not-store-this-secret", raw)
        self.assertEqual(envelope["write_policy"], "preserve_manual_and_nonempty")

    @mock.patch.object(app.requests, "get")
    @mock.patch.object(app.requests, "post")
    def test_windows_ai_lane_returns_only_certified_package_photos(self, post, get):
        ai_root = Path(self.temp.name) / "ai-packages"
        photos = ai_root / "A203781" / "photos"
        photos.mkdir(parents=True)
        (photos / "01.jpg").write_bytes(b"clean")
        app.AI_ENDPOINT = "http://127.0.0.1:8793"
        app.AI_PACKAGES_ROOT = ai_root
        post.return_value = FakeResponse(payload={"job_id": "a" * 32})
        get.return_value = FakeResponse(payload={"job_id": "a" * 32, "status": "ready", "internal_id": "A203781"})
        result = app.ai_package_photos(self.payload())
        self.assertEqual(result, [photos / "01.jpg"])
        self.assertEqual(post.call_args.kwargs["json"]["value"], "203781")

    @mock.patch.object(app.requests, "get")
    def test_remote_ai_package_downloads_certified_photos(self, get):
        app.AI_ENDPOINT = "https://windows-ai.example"
        get.side_effect = [
            FakeResponse(content=b"clean" * 1024, content_type="image/jpeg"),
            FakeResponse(content=b"clean2" * 1024, content_type="image/jpeg"),
        ]
        photos = app.download_remote_ai_photos("203781", 2, {"X-Block3-Token": "secret"})
        self.assertEqual([path.name for path in photos], ["01.jpg", "02.jpg"])
        self.assertTrue(all(path.is_file() for path in photos))
        self.assertEqual(get.call_args_list[0].kwargs["headers"]["X-Block3-Token"], "secret")

    @mock.patch.object(app, "ai_package_photos")
    @mock.patch.object(app.requests, "get")
    def test_expired_cdn_uses_preserved_block2_originals(self, get, ai_photos):
        get.side_effect = app.requests.RequestException("expired")
        source_root = Path(self.temp.name) / "block2-listings"
        preserved = source_root / "olx" / "203781" / "original"
        preserved.mkdir(parents=True)
        (preserved / "01.jpg").write_bytes(TEST_IMAGE)
        clean = Path(self.temp.name) / "certified" / "01.jpg"
        clean.parent.mkdir(parents=True)
        clean.write_bytes(CERTIFIED_IMAGE)
        app.SOURCE_LISTINGS_ROOT = source_root
        app.AI_ENDPOINT = "http://127.0.0.1:8793"
        ai_photos.return_value = [clean]
        result = app.save_approved_photos("203781", self.payload()["images"], self.payload())
        original = Path(result[0]["original_path"])
        self.assertEqual(original.read_bytes(), TEST_IMAGE)
        self.assertEqual(Path(result[0]["final_path"]).read_bytes(), CERTIFIED_IMAGE)

    @mock.patch.object(app.time, "sleep")
    @mock.patch.object(app.requests, "get")
    @mock.patch.object(app.requests, "post")
    def test_windows_ai_poll_tolerates_busy_worker_timeout(self, post, get, _sleep):
        ai_root = Path(self.temp.name) / "ai-packages"
        photos = ai_root / "A203781" / "photos"
        photos.mkdir(parents=True)
        (photos / "01.jpg").write_bytes(b"clean")
        app.AI_ENDPOINT = "http://127.0.0.1:8793"
        app.AI_PACKAGES_ROOT = ai_root
        post.return_value = FakeResponse(payload={"job_id": "a" * 32})
        get.side_effect = [app.requests.RequestException("busy"), FakeResponse(payload={"status": "ready", "internal_id": "A203781"})]
        self.assertEqual(app.ai_package_photos(self.payload()), [photos / "01.jpg"])

    @mock.patch.object(app.requests, "get")
    @mock.patch.object(app.requests, "post")
    def test_windows_ai_reuses_last_certified_package_after_retry_failure(self, post, get):
        ai_root = Path(self.temp.name) / "ai-packages"
        photos = ai_root / "B95B6E5C759" / "photos"
        photos.mkdir(parents=True)
        (photos / "01.jpg").write_bytes(b"certified")
        app.AI_ENDPOINT = "http://127.0.0.1:8793"
        app.AI_PACKAGES_ROOT = ai_root
        post.return_value = FakeResponse(payload={"job_id": "b" * 32})
        get.return_value = FakeResponse(payload={"status": "failed", "internal_id": "B95B6E5C759", "error": "two source files failed"})
        self.assertEqual(app.ai_package_photos(self.payload()), [photos / "01.jpg"])

    def test_wsgi_health_and_existing_interface(self):
        captured = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        health = b"".join(app.app({"PATH_INFO": "/health", "REQUEST_METHOD": "GET", "wsgi.input": BytesIO()}, start_response))
        self.assertEqual(captured["status"], "200 OK")
        self.assertTrue(json.loads(health)["ok"])
        page = b"".join(app.app({"PATH_INFO": "/", "REQUEST_METHOD": "GET", "wsgi.input": BytesIO()}, start_response))
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn(b"KYIV ESTATE", page)
        self.assertNotIn(b"Fast import with source photos", page)
        self.assertNotIn(b"Removes people, agency images and watermarks", page)

    def test_pdf_places_logo_after_first_property_photo(self):
        pixel = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
        photo_root = app.PACKAGES_ROOT / "203781" / "photos"
        photo_root.mkdir(parents=True)
        first, second = photo_root / "01.png", photo_root / "02.png"
        first.write_bytes(pixel)
        second.write_bytes(pixel)
        app.LOGO_PATH.write_bytes(pixel)
        payload = self.payload()
        payload.update({"processing_mode": "ai", "language": "en", "title": "Apartment", "text": "Description"})
        with mock.patch("reportlab.platypus.SimpleDocTemplate") as document, mock.patch("reportlab.platypus.Image") as image:
            document.return_value.build.return_value = None
            app.make_pdf(payload)
        sources = [str(call.args[0]) for call in image.call_args_list]
        self.assertEqual(sources[:3], [str(first), str(app.DATA_ROOT / "assets" / "kyiv-estate-logo.jpg"), str(second)])


if __name__ == "__main__":
    unittest.main()
