"""
Location Data Classifier v2

Uses a scoring system to identify pages that actually contain location data:
1. URL/Title context - prioritize pages likely to have location data
2. Multi-signal scoring - require minimum score to classify as location page
3. Exclude low-value signals (generic search forms, footer addresses)
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from bs4 import BeautifulSoup
from collections import defaultdict


@dataclass
class LocationSignal:
    """A detected location signal on a page."""
    signal_type: str
    confidence: str  # high, medium, low
    points: int  # Score contribution
    details: str
    evidence: str = ""


@dataclass 
class PageClassification:
    """Classification results for a single page."""
    url: str
    title: str
    signals: List[LocationSignal] = field(default_factory=list)
    total_score: int = 0
    is_location_page: bool = False
    url_priority: str = ""  # high, low, neutral
    

@dataclass
class CarrierReport:
    """Classification report for an entire carrier."""
    carrier_name: str
    domain: str
    total_pages: int
    location_pages: int  # Pages that pass the threshold
    modalities_found: Dict[str, int] = field(default_factory=dict)
    top_pages: List[PageClassification] = field(default_factory=list)
    recommended_approach: str = ""


# Minimum score to be considered a location page
LOCATION_PAGE_THRESHOLD = 3


class LocationClassifier:
    """Classifies HTML pages for location data using scoring system."""
    
    # URL patterns that suggest INDEX/LIST pages (HIGHEST priority)
    # These are the main pages that list ALL locations
    INDEX_URL_PATTERNS = [
        r'/locations/?$',           # swifttrans.com/locations
        r'/locations\.html',        # usfoods.com/locations.html
        r'/Our-Locations',          # performancefoodservice.com/Our-Locations
        r'/our-locations/?$',
        r'/all-locations/?$',
        r'centersResult',           # sefl.com/seflWebsite/about/centersResult.jsp
        r'/coverage',               # arcb.com/shippers/coverage-area/
        r'/terminals/?$',           # ends with /terminals
        r'/terminals$',             # saia.com/tools-and-resources/terminals
        r'terminals/?$',            # any URL ending in terminals
        r'/service-centers/?$',
        r'/service-center/?$',
        r'service-center-locator',  # odfl service center locator
        r'/service-locations',      # universalintermodal.com/customers/service-locations
        r'/facilities/?$',
        r'/branches/?$',
        r'/find-us/?$',
        r'/terminal-locations/?$',
        r'/branch-locator',         # dbschenker branch-locator
        r'/map\.html',              # wallstreetsystems.com/map.html
        r'servicemap\.pdf',         # centraltransport servicemap.pdf
        r'/locator/?$',             # generic locator pages
        r'/find-location',
        r'/store-locator',
        r'/dealer-locator',
    ]
    
    # URL patterns that suggest location content (HIGH priority)
    # REMOVED: 'network', 'coverage', 'warehouse' - too broad, match non-location pages
    HIGH_PRIORITY_URL_PATTERNS = [
        r'location', r'terminal', r'service.?center', r'facilit', 
        r'branch', r'office', r'find.?us', r'locator', r'finder',
        r'yard', r'depot', r'where.?we', r'map\.html', r'servicemap'
    ]
    
    # URL patterns that suggest NON-location content (LOW priority)
    LOW_PRIORITY_URL_PATTERNS = [
        r'investor', r'career', r'job', r'blog', r'news', r'press',
        r'SEC', r'earning', r'stock', r'annual.?report', r'quarter',
        r'privacy', r'terms', r'legal', r'cookie', r'login', r'sign.?in',
        r'cart', r'checkout', r'account'
    ]
    
    # URL patterns for NON-US pages (should be deprioritized)
    NON_US_URL_PATTERNS = [
        r'/eu/', r'/europe/', r'/fr/', r'/de/', r'/es/', r'/uk/', r'/gb/',
        r'/global/', r'/international/', r'/asia/', r'/apac/', r'/latam/',
        r'/fr-', r'/de-', r'/es-', r'/it-', r'/nl-', r'/pt-',  # language codes
        r'\.fr/', r'\.de/', r'\.es/', r'\.co\.uk/', r'\.eu/',  # country TLDs in path
        r'europe\.',  # europe.xpo.com subdomain
    ]
    
    # URL patterns for US pages (should be boosted)
    US_URL_PATTERNS = [
        r'/us/', r'/en-us/', r'/united-states/', r'/usa/',
        r'/en/', r'\.com/', r'\.com$'  # .com without regional paths is US-default
    ]
    
    # Title patterns for location pages
    LOCATION_TITLE_KEYWORDS = [
        'location', 'terminal', 'service center', 'facility', 'branch',
        'office', 'find us', 'contact', 'where we', 'coverage', 'network'
    ]
    
    # Address patterns
    ADDRESS_PATTERNS = [
        # Full US address: 123 Main St, City, ST 12345
        r'\d{1,5}\s+[\w\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Way|Lane|Ln|Highway|Hwy|Parkway|Pkwy)\.?[,\s]+[\w\s]+,?\s*[A-Z]{2}\s*\d{5}',
        # City, State ZIP
        r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2}\s+\d{5}',
    ]
    
    # Map library signatures
    MAP_LIBRARIES = {
        'google_maps': ['maps.google.com', 'maps.googleapis.com', 'google.com/maps'],
        'mapbox': ['mapbox.com', 'mapboxgl', 'mapbox-gl'],
        'leaflet': ['leafletjs.com', 'L.map', 'leaflet.js'],
        'arcgis': ['arcgis.com', 'esri.com', 'FeatureServer', 'MapServer'],
    }
    
    def __init__(self):
        self.compiled_address_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.ADDRESS_PATTERNS
        ]
        self.index_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.INDEX_URL_PATTERNS
        ]
        self.high_priority_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.HIGH_PRIORITY_URL_PATTERNS
        ]
        self.low_priority_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.LOW_PRIORITY_URL_PATTERNS
        ]
        self.non_us_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.NON_US_URL_PATTERNS
        ]
        self.us_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.US_URL_PATTERNS
        ]
    
    def _get_url_priority(self, url: str, title: str) -> Tuple[str, int]:
        """Determine URL priority and bonus points."""
        url_lower = url.lower()
        title_lower = title.lower()
        
        # Check for low priority first (investor pages, careers, etc.)
        for pattern in self.low_priority_patterns:
            if pattern.search(url_lower) or pattern.search(title_lower):
                return ('low', -5)  # Strong penalty for likely non-location pages
        
        # Check for NON-US pages - penalize (but don't disqualify)
        is_non_us = any(p.search(url_lower) for p in self.non_us_patterns)
        is_us = any(p.search(url_lower) for p in self.us_patterns)
        us_penalty = -3 if is_non_us and not is_us else 0
        
        # Check for INDEX pages (e.g., /locations, /terminals) - HIGHEST priority
        for pattern in self.index_patterns:
            if pattern.search(url_lower):
                return ('index', 10 + us_penalty)  # BIG bonus for main location index pages
        
        # Check for high priority (location-related URLs)
        for pattern in self.high_priority_patterns:
            if pattern.search(url_lower):
                return ('high', 2 + us_penalty)  # Bonus for location-like URL
        
        # Check title for location keywords
        for keyword in self.LOCATION_TITLE_KEYWORDS:
            if keyword in title_lower:
                return ('high', 2 + us_penalty)
        
        return ('neutral', us_penalty)
    
    def classify_html(self, html: str, url: str = "", filename: str = "") -> PageClassification:
        """Classify a single HTML page - NEW APPROACH with primary signal requirement."""
        soup = BeautifulSoup(html, 'lxml')
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        
        classification = PageClassification(url=url or filename, title=title)
        url_lower = (url or filename).lower()
        
        # ============================================
        # STEP 1: CHECK DISQUALIFIERS (auto-reject)
        # ============================================
        
        # Check for 404/error pages
        if self._is_error_page(html, soup, title):
            classification.is_location_page = False
            classification.total_score = 0
            classification.signals = [LocationSignal(
                signal_type='DISQUALIFIED',
                confidence='high',
                points=0,
                details='Error/404 page',
                evidence=title[:50]
            )]
            return classification
        
        # Check for non-English pages (unless URL suggests location content)
        if self._is_non_english(soup) and not self._has_location_url(url_lower):
            classification.is_location_page = False
            classification.total_score = 0
            classification.signals = [LocationSignal(
                signal_type='DISQUALIFIED',
                confidence='high',
                points=0,
                details='Non-English page',
                evidence='lang attribute or content'
            )]
            return classification
        
        # Check for quote/lead/career pages (by URL pattern)
        if self._is_excluded_page_type(url_lower, title):
            classification.is_location_page = False
            classification.total_score = 0
            classification.signals = [LocationSignal(
                signal_type='DISQUALIFIED',
                confidence='high',
                points=0,
                details='Quote/career/excluded page type',
                evidence=url[:50]
            )]
            return classification
        
        # ============================================
        # STEP 2: CHECK FOR PRIMARY SIGNALS (must have one)
        # ============================================
        signals = []
        has_primary_signal = False
        total_score = 0
        
        # PRIMARY 1: Location-specific URL (INDEX pages)
        is_index_page = any(p.search(url_lower) for p in self.index_patterns)
        if is_index_page:
            has_primary_signal = True
            signals.append(LocationSignal(
                signal_type='INDEX_PAGE',
                confidence='high',
                points=15,
                details='URL is a location index page',
                evidence=url[:80]
            ))
        
        # PRIMARY 2: Multiple addresses (5+ = definitely a location list)
        address_count, address_signal = self._count_real_addresses(html, soup)
        if address_count >= 5:
            has_primary_signal = True
            signals.append(address_signal)
        elif address_count >= 2:
            # 2-4 addresses - could be location page, add as secondary
            signals.append(address_signal)
        
        # PRIMARY 3: Map with multiple markers (coordinate data)
        coord_count, coord_signal = self._count_coordinates(html)
        if coord_count >= 3:
            has_primary_signal = True
            signals.append(coord_signal)
        elif coord_count >= 1:
            signals.append(coord_signal)
        
        # PRIMARY 4: Google Maps embed with location data
        maps_signal = self._detect_google_maps_strict(html, soup)
        if maps_signal and maps_signal.points >= 5:
            has_primary_signal = True
            signals.append(maps_signal)
        elif maps_signal:
            signals.append(maps_signal)
        
        # PRIMARY 5: Location finder/locator form
        locator_signal = self._detect_location_finder_form(html, soup)
        if locator_signal:
            has_primary_signal = True
            signals.append(locator_signal)
        
        # ============================================
        # STEP 3: If no primary signal, check URL context
        # ============================================
        if not has_primary_signal:
            # Check if URL suggests location content
            if self._has_location_url(url_lower):
                # URL suggests location - give it a chance with lower score
                signals.append(LocationSignal(
                    signal_type='URL_CONTEXT',
                    confidence='medium',
                    points=3,
                    details='URL suggests location content',
                    evidence=url[:60]
                ))
                has_primary_signal = True  # Allow through with URL context
        
        # ============================================
        # STEP 4: If no primary signal, reject
        # ============================================
        if not has_primary_signal:
            classification.is_location_page = False
            classification.total_score = 0
            classification.signals = signals if signals else [LocationSignal(
                signal_type='NO_LOCATION_CONTENT',
                confidence='high',
                points=0,
                details='No primary location signals found',
                evidence=''
            )]
            return classification
        
        # ============================================
        # STEP 5: Add secondary signals (bonus points)
        # ============================================
        
        # JSON-LD with multiple locations
        json_signal = self._detect_json_ld_strict(html, soup)
        if json_signal:
            signals.append(json_signal)
        
        # Interactive maps (Mapbox, Leaflet, ArcGIS)
        map_signals = self._detect_interactive_maps(html, soup)
        signals.extend(map_signals)
        
        # Location iframes
        iframe_signals = self._detect_location_iframes(html, soup)
        signals.extend(iframe_signals)
        
        # PDF links with location content
        pdf_signals = self._detect_pdf_links(html, soup)
        signals.extend(pdf_signals)
        
        # Calculate total score
        for signal in signals:
            total_score += signal.points
        
        classification.signals = signals
        classification.total_score = max(0, total_score)  # Don't go negative
        classification.is_location_page = total_score >= LOCATION_PAGE_THRESHOLD
        
        return classification
    
    # ============================================
    # NEW HELPER METHODS FOR V2 CLASSIFIER
    # ============================================
    
    def _is_error_page(self, html: str, soup: BeautifulSoup, title: str) -> bool:
        """Check if page is a 404 or error page."""
        title_lower = title.lower() if title else ''
        
        # Check title for error indicators
        error_titles = ['404', 'not found', 'page not found', 'error', 'oops', 
                       'page does not exist', 'page doesn\'t exist']
        if any(err in title_lower for err in error_titles):
            return True
        
        # Check for error status in HTML
        html_lower = html.lower()
        if '<h1>404' in html_lower or '<h1>page not found' in html_lower:
            return True
        
        # Check for very short HTML (likely error page)
        if len(html) < 2000:
            return True
        
        return False
    
    def _is_non_english(self, soup: BeautifulSoup) -> bool:
        """Check if page is non-English."""
        html_tag = soup.find('html')
        if html_tag:
            lang = html_tag.get('lang', '').lower()
            if lang and not lang.startswith('en'):
                return True
        return False
    
    def _has_location_url(self, url_lower: str) -> bool:
        """Check if URL suggests location content."""
        # STRICT location keywords - removed 'coverage', 'network', 'warehouse', 'office'
        location_keywords = ['location', 'terminal', 'service-center', 'service-location',
                            'facility', 'branch', 'find-us', 'depot', 'yard', 
                            'loadboard', 'load-board', '/map', 'locator', 'finder',
                            'servicemap', 'branch-locator', 'store-locator']
        return any(kw in url_lower for kw in location_keywords)
    
    def _is_excluded_page_type(self, url_lower: str, title: str) -> bool:
        """Check if page type should be excluded (quote, career, etc.)."""
        title_lower = title.lower() if title else ''
        
        exclude_patterns = [
            'quote', 'get-a-quote', 'instant-quote', 'request-quote',
            'career', 'job', 'apply', 'hiring',
            'login', 'signin', 'register', 'signup',
            'blog', 'news', 'press-release', 'article',
            'investor', 'annual-report', 'earnings',
            'privacy', 'terms', 'cookie', 'legal',
            'cart', 'checkout', 'order'
        ]
        
        if any(ex in url_lower for ex in exclude_patterns):
            return True
        if any(ex in title_lower for ex in exclude_patterns):
            return True
        
        return False
    
    def _count_real_addresses(self, html: str, soup: BeautifulSoup) -> tuple:
        """Count real addresses in main content (excluding footer/header)."""
        # Create a copy and remove header/footer/nav
        soup_copy = BeautifulSoup(str(soup), 'lxml')
        for elem in soup_copy.find_all(['header', 'footer', 'nav']):
            elem.decompose()
        for elem in soup_copy.find_all(class_=re.compile(r'footer|header|nav-|navbar|menu|copyright', re.I)):
            elem.decompose()
        
        text = soup_copy.get_text(separator=' ')
        
        addresses_found = set()
        for pattern in self.compiled_address_patterns:
            matches = pattern.findall(text)
            for match in matches:
                clean = match.strip()
                if len(clean) > 15:
                    addresses_found.add(clean)
        
        count = len(addresses_found)
        
        if count >= 5:
            return count, LocationSignal(
                signal_type='ADDRESS_LIST',
                confidence='high',
                points=10,
                details=f'{count} addresses found - location listing page',
                evidence='; '.join(list(addresses_found)[:3])
            )
        elif count >= 2:
            return count, LocationSignal(
                signal_type='ADDRESS_PAIR',
                confidence='medium',
                points=3,
                details=f'{count} addresses found',
                evidence='; '.join(list(addresses_found)[:2])
            )
        
        return count, None
    
    def _count_coordinates(self, html: str) -> tuple:
        """Count coordinate pairs in HTML."""
        # Look for lat/lng patterns
        coord_pattern = r'(?:lat|latitude)["\']?\s*[:=]\s*(-?\d{1,3}\.\d{3,})'
        lat_matches = re.findall(coord_pattern, html, re.IGNORECASE)
        
        coord_pattern2 = r'LatLng\s*\(\s*(-?\d{1,3}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)\s*\)'
        latlng_matches = re.findall(coord_pattern2, html)
        
        count = len(set(lat_matches)) + len(set(latlng_matches))
        
        if count >= 3:
            return count, LocationSignal(
                signal_type='COORDINATE_DATA',
                confidence='high',
                points=8,
                details=f'{count} coordinate markers found',
                evidence='Multiple lat/lng coordinates'
            )
        elif count >= 1:
            return count, LocationSignal(
                signal_type='COORDINATE_DATA',
                confidence='medium',
                points=2,
                details=f'{count} coordinate(s) found',
                evidence='lat/lng data'
            )
        
        return count, None
    
    def _detect_google_maps_strict(self, html: str, soup: BeautifulSoup) -> LocationSignal:
        """Detect REAL Google Maps embeds (not tag manager, recaptcha, etc.).
        
        CRITICAL: A single map showing just the corporate HQ is NOT a location page.
        We need evidence of MULTIPLE markers or location-rich content.
        """
        html_lower = html.lower()
        
        # STRICT patterns - must be actual map embeds, not Google services
        real_map_patterns = [
            'google.com/maps/embed',      # Embed URL
            '/maps/d/embed',              # My Maps embed
            'maps.google.com/maps?',      # Direct maps link
            'google.com/maps?q=',         # Maps with query
            'google.com/maps/place',      # Place embed
        ]
        
        # EXCLUDE these Google services (false positives)
        exclude_patterns = [
            'googletagmanager',
            'recaptcha',
            'analytics',
            'gtag',
            'fonts.googleapis',
            'ajax.googleapis',
        ]
        
        # Check if we have a real map
        has_real_map = any(p in html_lower for p in real_map_patterns)
        
        # Count Google Maps links (MULTIPLE = location list, SINGLE = just HQ)
        maps_links = soup.find_all('a', href=re.compile(r'google\.com/maps|maps\.google\.com', re.I))
        maps_link_count = len(maps_links)
        
        if has_real_map:
            # Check iframes for actual map embeds
            iframes = soup.find_all('iframe')
            for iframe in iframes:
                src = (iframe.get('src') or '').lower()
                if any(p in src for p in real_map_patterns):
                    if not any(e in src for e in exclude_patterns):
                        # Single embed with no other location signals = likely just HQ map
                        # Give lower points unless there are other location signals
                        points = 3 if maps_link_count < 3 else 5
                        return LocationSignal(
                            signal_type='GOOGLE_MAPS_EMBED',
                            confidence='medium' if points == 3 else 'high',
                            points=points,
                            details=f'Google Maps embed iframe (links={maps_link_count})',
                            evidence=src[:80]
                        )
            
            # Multiple Google Maps links = definitely a location list
            if maps_link_count >= 3:
                return LocationSignal(
                    signal_type='GOOGLE_MAPS_LINK',
                    confidence='high',
                    points=5,
                    details=f'{maps_link_count} Google Maps links',
                    evidence='Multiple maps.google.com links'
                )
            elif maps_link_count == 1:
                # Single link = likely just HQ - very low value
                return LocationSignal(
                    signal_type='GOOGLE_MAPS_LINK',
                    confidence='low',
                    points=1,
                    details='Single Google Maps link (likely HQ only)',
                    evidence='maps.google.com link'
                )
        
        # Check for Maps JavaScript API
        if 'maps.googleapis.com/maps/api/js' in html_lower:
            # Check for various map initialization patterns
            map_init_patterns = ['new google.maps.map', 'google.maps.marker', 
                                'google.maps.infowindow', 'initmap', 'mapinit', 'loadmap']
            if any(p in html_lower for p in map_init_patterns):
                # Check for multiple markers (evidence of multiple locations)
                marker_count = html_lower.count('google.maps.marker')
                marker_count += html_lower.count('addmarker')
                marker_count += html_lower.count('new marker')
                
                # Also count LatLng which indicates markers
                latlng_count = len(re.findall(r'latlng\s*\(', html_lower))
                
                if marker_count >= 3 or latlng_count >= 3:
                    return LocationSignal(
                        signal_type='GOOGLE_MAPS_API',
                        confidence='high',
                        points=5,
                        details=f'Google Maps API with multiple markers (~{max(marker_count, latlng_count)})',
                        evidence='maps.googleapis.com with multiple markers'
                    )
                else:
                    # Single marker = likely just HQ
                    return LocationSignal(
                        signal_type='GOOGLE_MAPS_API',
                        confidence='low',
                        points=2,
                        details='Google Maps API (possibly single marker)',
                        evidence='maps.googleapis.com'
                    )
        
        return None
    
    def _detect_location_finder_form(self, html: str, soup: BeautifulSoup) -> LocationSignal:
        """Detect actual location finder forms (not quote forms)."""
        forms = soup.find_all('form')
        
        for form in forms:
            form_html = str(form).lower()
            form_action = form.get('action', '').lower()
            form_text = form.get_text().lower()
            
            # MUST have location finder language
            finder_keywords = ['find location', 'find terminal', 'find facility',
                              'locate', 'search location', 'find near', 'nearby',
                              'service center locator', 'terminal locator']
            
            has_finder_language = any(kw in form_html or kw in form_text for kw in finder_keywords)
            
            # MUST have radius/distance (quote forms don't have this)
            has_radius = any(r in form_html for r in ['radius', 'distance', 'miles', 'within'])
            
            # Should NOT have quote-related content
            is_quote_form = any(q in form_html or q in form_action for q in 
                               ['quote', 'lead', 'contact', 'order', 'ship'])
            
            if has_finder_language and not is_quote_form:
                return LocationSignal(
                    signal_type='LOCATION_FINDER',
                    confidence='high',
                    points=8,
                    details='Location finder/locator form',
                    evidence='Form with location search capability'
                )
            elif has_radius and not is_quote_form:
                return LocationSignal(
                    signal_type='LOCATION_SEARCH',
                    confidence='medium',
                    points=4,
                    details='Search form with radius/distance',
                    evidence='Form with radius search'
                )
        
        return None
    
    def _detect_json_ld_strict(self, html: str, soup: BeautifulSoup) -> LocationSignal:
        """Detect JSON-LD with MULTIPLE locations (not just company HQ)."""
        scripts = soup.find_all('script', type='application/ld+json')
        location_count = 0
        
        for script in scripts:
            try:
                data = json.loads(script.string or '{}')
                data_str = json.dumps(data).lower()
                
                # Count distinct addresses
                location_count += data_str.count('"streetaddress"')
                location_count += data_str.count('"postalcode"')
                
            except:
                continue
        
        # Need multiple locations (not just one company address)
        if location_count >= 4:  # 2+ locations (each has street + postal)
            return LocationSignal(
                signal_type='JSON_LD_LOCATIONS',
                confidence='high',
                points=5,
                details=f'JSON-LD with {location_count//2}+ locations',
                evidence='Structured data with multiple addresses'
            )
        
        return None
    
    def _detect_text_addresses(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect multiple addresses in main content (not footer/header)."""
        signals = []
        
        # Create a copy to avoid modifying original
        soup_copy = BeautifulSoup(str(soup), 'lxml')
        
        # Remove header, footer, nav elements
        for elem in soup_copy.find_all(['header', 'footer', 'nav']):
            elem.decompose()
        for elem in soup_copy.find_all(class_=re.compile(r'footer|header|nav-|navbar|menu|copyright', re.I)):
            elem.decompose()
        
        text = soup_copy.get_text(separator=' ')
        
        addresses_found = set()
        for pattern in self.compiled_address_patterns:
            matches = pattern.findall(text)
            for match in matches:
                clean = match.strip()
                if len(clean) > 15:  # Reasonable address length
                    addresses_found.add(clean)
        
        # Scoring based on number of addresses - MORE addresses = MUCH higher score
        # This prioritizes INDEX pages that list all locations
        if len(addresses_found) >= 10:
            signals.append(LocationSignal(
                signal_type='LOCATION_INDEX',
                confidence='high',
                points=15,  # HUGE bonus - this is likely THE main locations page
                details=f"Found {len(addresses_found)} addresses - likely main location index",
                evidence='; '.join(list(addresses_found)[:5])
            ))
        elif len(addresses_found) >= 5:
            signals.append(LocationSignal(
                signal_type='ADDRESS_LIST',
                confidence='high',
                points=8,  # Strong signal - multiple addresses
                details=f"Found {len(addresses_found)} addresses",
                evidence='; '.join(list(addresses_found)[:3])
            ))
        elif len(addresses_found) >= 3:
            signals.append(LocationSignal(
                signal_type='ADDRESS_LIST',
                confidence='medium',
                points=4,
                details=f"Found {len(addresses_found)} addresses",
                evidence='; '.join(list(addresses_found)[:3])
            ))
        # 1-2 addresses = 0 points (individual location pages, not useful for index)
        
        return signals
    
    def _detect_google_maps(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect Google Maps embeds and links."""
        signals = []
        
        # Check for Google Maps iframe (strong signal)
        iframes = soup.find_all('iframe')
        maps_iframes = 0
        for iframe in iframes:
            src = iframe.get('src', '')
            if 'google.com/maps' in src or 'maps.google.com' in src:
                maps_iframes += 1
        
        if maps_iframes > 0:
            signals.append(LocationSignal(
                signal_type='GOOGLE_MAPS_EMBED',
                confidence='high',
                points=3,
                details=f'{maps_iframes} Google Maps iframe(s)',
                evidence='maps.google.com iframe'
            ))
        
        # Check for Google Maps JavaScript API
        if 'maps.googleapis.com/maps/api/js' in html:
            signals.append(LocationSignal(
                signal_type='GOOGLE_MAPS_API',
                confidence='high',
                points=3,
                details='Google Maps JavaScript API',
                evidence='maps.googleapis.com/maps/api/js'
            ))
        
        # Check for multiple Google Maps links (suggests location list)
        links = soup.find_all('a', href=True)
        maps_links = []
        for link in links:
            href = link.get('href', '')
            if 'google.com/maps' in href or 'maps.google.com' in href:
                maps_links.append(href)
        
        if len(maps_links) >= 3:
            signals.append(LocationSignal(
                signal_type='GOOGLE_MAPS_LINKS',
                confidence='high',
                points=3,
                details=f'{len(maps_links)} links to Google Maps',
                evidence=f'{len(maps_links)} map links found'
            ))
        elif len(maps_links) == 1:
            signals.append(LocationSignal(
                signal_type='GOOGLE_MAPS_LINK',
                confidence='low',
                points=1,
                details='Single link to Google Maps',
                evidence=maps_links[0][:100]
            ))
        
        return signals
    
    def _detect_interactive_maps(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect interactive map libraries (Mapbox, Leaflet, ArcGIS)."""
        signals = []
        html_lower = html.lower()
        
        for lib_name, signatures in self.MAP_LIBRARIES.items():
            if lib_name == 'google_maps':
                continue  # Handled separately
            
            for sig in signatures:
                if sig.lower() in html_lower:
                    signals.append(LocationSignal(
                        signal_type=f'MAP_{lib_name.upper()}',
                        confidence='high',
                        points=3,
                        details=f'{lib_name.replace("_", " ").title()} map detected',
                        evidence=sig
                    ))
                    break
        
        return signals
    
    def _detect_clickable_lists(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect clickable lists that reveal location data on interaction."""
        signals = []
        html_lower = html.lower()
        
        # STRICT location keywords - avoid generic terms like "service"
        location_keywords = ['terminal', 'location', 'facility', 'branch', 'service-center', 
                            'service center', 'drop yard', 'warehouse', 'depot']
        
        # Find elements with click handlers containing location data attributes
        clickable_elements = soup.find_all(attrs={'data-location': True})
        clickable_elements.extend(soup.find_all(attrs={'data-terminal': True}))
        clickable_elements.extend(soup.find_all(attrs={'data-marker': True}))
        clickable_elements.extend(soup.find_all(attrs={'data-lat': True}))
        clickable_elements.extend(soup.find_all(attrs={'data-lng': True}))
        
        # Only count accordions/tabs that are SPECIFICALLY location listings
        # Must have class containing location keyword OR be inside a location container
        accordions = soup.find_all(class_=re.compile(r'location.*accordion|terminal.*list|facility.*item', re.I))
        tabs = soup.find_all(class_=re.compile(r'location.*tab|terminal.*tab|location-item', re.I))
        
        location_clicks = []
        for elem in clickable_elements:
            elem_text = str(elem).lower()
            if any(kw in elem_text for kw in location_keywords):
                location_clicks.append(elem.get('data-location', elem.get('data-terminal', 'marker')))
        
        # Only count as location-related if the accordion/tab contains MULTIPLE location keywords
        location_interactives = len([a for a in accordions + tabs 
                                     if sum(1 for kw in location_keywords if kw in str(a).lower()) >= 2])
        
        # Require MORE evidence - either data attributes OR strongly typed location elements
        if len(location_clicks) >= 5 or (len(accordions) + len(tabs) >= 3 and location_interactives >= 3):
            signals.append(LocationSignal(
                signal_type='CLICKABLE_LIST',
                confidence='high',
                points=5,
                details=f'Clickable location list ({len(location_clicks)} data markers, {len(accordions)+len(tabs)} location accordions)',
                evidence='Interactive elements reveal location data'
            ))
        
        # Also detect list structures with many location items
        location_lists = soup.find_all(['ul', 'ol', 'div'], class_=re.compile(r'location|terminal|facility|branch', re.I))
        if location_lists:
            for lst in location_lists:
                items = lst.find_all(['li', 'div', 'a'])
                if len(items) >= 5:
                    signals.append(LocationSignal(
                        signal_type='LOCATION_LIST_STRUCTURE',
                        confidence='medium',
                        points=4,
                        details=f'Location list structure with {len(items)} items',
                        evidence=f'List class: {lst.get("class", ["unknown"])}'
                    ))
                    break
        
        return signals
    
    def _detect_pdf_links(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect PDF links that might contain location data."""
        signals = []
        
        links = soup.find_all('a', href=True)
        high_value_pdfs = []  # servicemap, terminal-map, etc.
        regular_pdfs = []
        
        # HIGH VALUE: These PDFs are specifically maps/location lists
        high_value_keywords = ['servicemap', 'service-map', 'terminal-map', 'location-map',
                              'coverage-map', 'network-map', 'facility-map', 'directory']
        
        # REGULAR: These might have location data
        regular_keywords = ['location', 'terminal', 'service', 'facility', 'map']
        
        for link in links:
            href = link.get('href', '').lower()
            text = link.get_text().lower()
            
            if '.pdf' in href:
                # Check for high-value map PDFs first
                if any(kw in href for kw in high_value_keywords):
                    high_value_pdfs.append(href)
                elif any(kw in href or kw in text for kw in regular_keywords):
                    regular_pdfs.append(href)
        
        if high_value_pdfs:
            signals.append(LocationSignal(
                signal_type='PDF_SERVICEMAP',
                confidence='high',
                points=8,  # HIGH value - servicemap.pdf is often THE source
                details=f"Service/location map PDF: {high_value_pdfs[0][:50]}",
                evidence='; '.join(high_value_pdfs[:2])
            ))
        elif regular_pdfs:
            signals.append(LocationSignal(
                signal_type='PDF_LOCATIONS',
                confidence='medium',
                points=2,
                details=f"{len(regular_pdfs)} location-related PDF(s)",
                evidence='; '.join(regular_pdfs[:2])
            ))
        
        return signals
    
    def _detect_location_search_forms(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect location-specific search forms (not quote/lead forms)."""
        signals = []
        
        forms = soup.find_all('form')
        for form in forms:
            form_html = str(form).lower()
            form_action = form.get('action', '').lower()
            form_id = form.get('id', '').lower()
            form_class = ' '.join(form.get('class', [])).lower()
            
            # EXCLUDE quote/lead/contact forms - these ask for zip but aren't location finders
            exclude_patterns = ['quote', 'lead', 'contact', 'signup', 'subscribe', 
                               'newsletter', 'apply', 'career', 'job', 'order', 'checkout']
            if any(ex in form_html or ex in form_action or ex in form_id or ex in form_class 
                   for ex in exclude_patterns):
                continue
            
            # Must have SPECIFIC location finder indicators
            location_finder_signals = [
                'find.*location', 'find.*terminal', 'find.*facility', 'find.*branch',
                'location.*search', 'terminal.*search', 'search.*location',
                'locator', 'find-us', 'near.*you', 'nearby'
            ]
            
            # Also check for radius/distance which are locator-specific (quotes don't need radius)
            radius_signals = ['radius', 'miles', 'distance', 'within']
            
            finder_matches = sum(1 for f in location_finder_signals if re.search(f, form_html))
            radius_matches = sum(1 for f in radius_signals if re.search(f, form_html))
            
            # Require location finder language OR radius fields (not just zip)
            if finder_matches >= 1 or radius_matches >= 2:
                signals.append(LocationSignal(
                    signal_type='LOCATION_SEARCH',
                    confidence='high',
                    points=3,
                    details='Location finder form',
                    evidence=f'{finder_matches} finder signals, {radius_matches} radius fields'
                ))
                break
        
        return signals
    
    def _detect_json_ld(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect JSON-LD with location schema types - must be specific to THIS page."""
        signals = []
        
        scripts = soup.find_all('script', type='application/ld+json')
        location_count = 0
        has_specific_location = False
        
        for script in scripts:
            try:
                data = json.loads(script.string or '{}')
                data_str = json.dumps(data).lower()
                
                # Count PostalAddress occurrences (more = better, 1 might just be company HQ)
                location_count += data_str.count('postaladdress')
                location_count += data_str.count('geocoordinates')
                
                # Check for specific location types (not just Organization)
                specific_types = ['localbusiness', 'store', 'warehouse', 'place', 
                                 'depot', 'terminal', 'servicecenter']
                if any(t in data_str for t in specific_types):
                    has_specific_location = True
                    
            except:
                continue
        
        # Only flag if multiple locations OR specific location type (not just company address)
        if location_count >= 2 or has_specific_location:
            signals.append(LocationSignal(
                signal_type='JSON_LD_LOCATION',
                confidence='high',
                points=3,
                details=f'JSON-LD with {location_count} location(s)',
                evidence='LocalBusiness/Place/PostalAddress schema'
            ))
        
        return signals
    
    def _detect_static_image_maps(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect static image maps that might contain location markers."""
        signals = []
        
        # STRICT map keywords - must be clearly a geographical map
        map_keywords = ['coverage-map', 'service-map', 'network-map', 'territory-map',
                       'location-map', 'terminal-map', 'service-area-map', 'facility-map']
        
        # Also check for map in context of locations (not generic "roadmap", "sitemap", etc.)
        map_context_keywords = ['coverage', 'service-area', 'territory', 'network']
        
        images = soup.find_all('img')
        map_images = []
        
        for img in images:
            src = img.get('src', '').lower()
            alt = img.get('alt', '').lower()
            title = img.get('title', '').lower()
            
            # Check for explicit map keywords
            if any(kw in src or kw in alt or kw in title for kw in map_keywords):
                map_images.append({
                    'src': img.get('src', ''),
                    'alt': img.get('alt', ''),
                })
            # Or "map" combined with location context
            elif 'map' in src or 'map' in alt:
                if any(ctx in src or ctx in alt for ctx in map_context_keywords):
                    map_images.append({
                        'src': img.get('src', ''),
                        'alt': img.get('alt', ''),
                    })
        
        if map_images:
            signals.append(LocationSignal(
                signal_type='STATIC_IMAGE_MAP',
                confidence='medium',
                points=3,
                details=f'{len(map_images)} static map image(s)',
                evidence=f"Images: {', '.join([m['alt'] or m['src'][:50] for m in map_images[:3]])}"
            ))
        
        return signals
    
    def _detect_api_endpoints(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect API endpoints that might serve location data."""
        signals = []
        html_lower = html.lower()
        
        # ArcGIS patterns
        arcgis_patterns = [
            'featureserver', 'mapserver', 'arcgis.com', 'arcgisonline.com',
            'rest/services', '/arcgis/'
        ]
        
        # WordPress REST API
        wp_patterns = [
            '/wp-json/', 'wp-api', '/wp/v2/'
        ]
        
        # General REST/API patterns for locations
        api_patterns = [
            '/api/locations', '/api/terminals', '/api/facilities',
            '/api/branches', '/api/stores', '/api/service-centers',
            'locations.json', 'terminals.json', 'stores.json',
            '/v1/locations', '/v2/locations'
        ]
        
        # Webflow CMS
        webflow_patterns = [
            'webflow.com', 'assets.website-files.com'
        ]
        
        found_apis = []
        
        for pattern in arcgis_patterns:
            if pattern in html_lower:
                found_apis.append(f'ArcGIS: {pattern}')
                
        for pattern in wp_patterns:
            if pattern in html_lower:
                found_apis.append(f'WordPress: {pattern}')
                
        for pattern in api_patterns:
            if pattern in html_lower:
                found_apis.append(f'REST API: {pattern}')
                
        for pattern in webflow_patterns:
            if pattern in html_lower:
                found_apis.append(f'Webflow: {pattern}')
        
        if found_apis:
            signals.append(LocationSignal(
                signal_type='API_ENDPOINT',
                confidence='high',
                points=5,
                details=f'{len(found_apis)} API endpoint(s) detected',
                evidence='; '.join(found_apis[:3])
            ))
        
        return signals
    
    def _detect_coordinate_data(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect latitude/longitude coordinate data in HTML/JS."""
        signals = []
        
        # Patterns for coordinates
        coord_patterns = [
            # lat/lng in JS objects: lat: 40.7128, lng: -74.0060
            r'lat[itude]*["\']?\s*[:=]\s*-?\d{1,3}\.\d+',
            r'lng|lon[gitude]*["\']?\s*[:=]\s*-?\d{1,3}\.\d+',
            # LatLng constructor
            r'LatLng\s*\(\s*-?\d{1,3}\.\d+\s*,\s*-?\d{1,3}\.\d+\s*\)',
            # GeoJSON
            r'"coordinates"\s*:\s*\[\s*-?\d{1,3}\.\d+',
            # data attributes
            r'data-lat[itude]*\s*=\s*["\']?-?\d{1,3}\.\d+',
            r'data-lng|lon[gitude]*\s*=\s*["\']?-?\d{1,3}\.\d+',
        ]
        
        coord_matches = []
        for pattern in coord_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            coord_matches.extend(matches[:5])  # Limit per pattern
        
        if len(coord_matches) >= 3:  # Need multiple coordinates to be significant
            signals.append(LocationSignal(
                signal_type='COORDINATE_DATA',
                confidence='high',
                points=4,
                details=f'{len(coord_matches)} coordinate references found',
                evidence=f"Examples: {', '.join(coord_matches[:3])}"
            ))
        
        return signals
    
    def _detect_location_iframes(self, html: str, soup: BeautifulSoup) -> List[LocationSignal]:
        """Detect iframes that might contain location/map content."""
        signals = []
        
        iframes = soup.find_all('iframe')
        location_iframes = []
        
        location_keywords = ['map', 'location', 'store', 'dealer', 'terminal', 
                           'branch', 'office', 'locator']
        
        for iframe in iframes:
            src = iframe.get('src', '').lower()
            title = iframe.get('title', '').lower()
            name = iframe.get('name', '').lower()
            
            # Skip Google Maps (handled separately)
            if 'google.com/maps' in src or 'maps.google' in src:
                continue
                
            if any(kw in src or kw in title or kw in name for kw in location_keywords):
                location_iframes.append({
                    'src': iframe.get('src', ''),
                    'title': iframe.get('title', '')
                })
        
        if location_iframes:
            signals.append(LocationSignal(
                signal_type='LOCATION_IFRAME',
                confidence='medium',
                points=3,
                details=f'{len(location_iframes)} location-related iframe(s)',
                evidence=f"Sources: {', '.join([i['src'][:60] for i in location_iframes[:2]])}"
            ))
        
        return signals
    
    def _extract_url_from_html(self, html: str, soup: BeautifulSoup) -> str:
        """Extract actual URL from HTML (canonical, og:url, or crawler-injected tag)."""
        # Try crawler-injected original URL first (most reliable)
        crawler_url = soup.find('meta', attrs={'name': 'crawler-original-url'})
        if crawler_url and crawler_url.get('content'):
            return crawler_url['content']
        
        # Try canonical link
        canonical = soup.find('link', rel='canonical')
        if canonical and canonical.get('href'):
            return canonical['href']
        
        # Try og:url
        og_url = soup.find('meta', property='og:url')
        if og_url and og_url.get('content'):
            return og_url['content']
        
        # Try twitter:url
        twitter_url = soup.find('meta', attrs={'name': 'twitter:url'})
        if twitter_url and twitter_url.get('content'):
            return twitter_url['content']
        
        return ""
    
    def classify_carrier(self, carrier_name: str, pages_dir: Path) -> CarrierReport:
        """Classify all pages for a carrier."""
        html_files = list(pages_dir.glob('*.html'))
        
        report = CarrierReport(
            carrier_name=carrier_name,
            domain=pages_dir.name,
            total_pages=len(html_files),
            location_pages=0
        )
        
        modality_counts = defaultdict(int)
        all_classifications = []
        
        for html_file in html_files:
            try:
                html = html_file.read_text(encoding='utf-8', errors='ignore')
                soup = BeautifulSoup(html, 'lxml')
                
                # Extract actual URL from HTML
                actual_url = self._extract_url_from_html(html, soup)
                if not actual_url:
                    actual_url = html_file.stem  # Fallback to filename
                
                classification = self.classify_html(html, url=actual_url, filename=html_file.stem)
                
                if classification.is_location_page:
                    report.location_pages += 1
                    all_classifications.append(classification)
                    
                    for signal in classification.signals:
                        if signal.points > 0:  # Only count positive signals
                            modality_counts[signal.signal_type] += 1
                        
            except Exception as e:
                continue
        
        # Sort by score and keep top pages
        all_classifications.sort(key=lambda x: x.total_score, reverse=True)
        report.top_pages = all_classifications[:20]
        report.modalities_found = dict(modality_counts)
        report.recommended_approach = self._generate_recommendation(modality_counts)
        
        return report
    
    def _generate_recommendation(self, modality_counts: Dict[str, int]) -> str:
        """Generate extraction approach recommendation."""
        if not modality_counts:
            return "No location data detected - manual review needed"
        
        recommendations = []
        
        if modality_counts.get('ADDRESS_LIST') or modality_counts.get('ADDRESS_PAIR'):
            recommendations.append("Parse addresses from HTML")
        
        if modality_counts.get('GOOGLE_MAPS_EMBED') or modality_counts.get('GOOGLE_MAPS_API'):
            recommendations.append("Extract from Google Maps embed")
        
        if modality_counts.get('GOOGLE_MAPS_LINKS'):
            recommendations.append("Parse Google Maps URLs")
        
        if any('MAP_' in k for k in modality_counts):
            recommendations.append("Query map API/data source")
        
        if modality_counts.get('PDF_LOCATIONS'):
            recommendations.append("Parse PDF documents")
        
        if modality_counts.get('LOCATION_SEARCH'):
            recommendations.append("Automate location search form")
        
        if modality_counts.get('JSON_LD_LOCATION'):
            recommendations.append("Parse JSON-LD structured data")
        
        return '; '.join(recommendations) if recommendations else "Manual review needed"


def classify_all_carriers(data_dir: Path) -> Dict[str, CarrierReport]:
    """Classify all crawled carriers."""
    classifier = LocationClassifier()
    reports = {}
    
    crawled_pages_dir = data_dir / 'crawled_pages'
    if not crawled_pages_dir.exists():
        print(f"No crawled pages found at {crawled_pages_dir}")
        return reports
    
    for carrier_dir in crawled_pages_dir.iterdir():
        if carrier_dir.is_dir():
            summary_file = carrier_dir / 'crawl_summary.json'
            if summary_file.exists():
                try:
                    summary = json.loads(summary_file.read_text())
                    carrier_name = summary.get('carrier_name', carrier_dir.name)
                except:
                    carrier_name = carrier_dir.name
            else:
                carrier_name = carrier_dir.name
            
            print(f"Classifying: {carrier_name}...")
            report = classifier.classify_carrier(carrier_name, carrier_dir)
            reports[carrier_name] = report
            
            print(f"  Location pages: {report.location_pages}/{report.total_pages}")
            if report.modalities_found:
                print(f"  Modalities: {dict(report.modalities_found)}")
    
    return reports


def generate_modality_report(reports: Dict[str, CarrierReport], output_path: Path) -> None:
    """Generate a modality report from classification results."""
    
    total_carriers = len(reports)
    carriers_with_data = sum(1 for r in reports.values() if r.location_pages > 0)
    
    # Aggregate modalities
    all_modalities = defaultdict(int)
    for report in reports.values():
        for modality, count in report.modalities_found.items():
            all_modalities[modality] += 1
    
    lines = [
        "=" * 80,
        "LOCATION DATA MODALITY REPORT",
        f"Score threshold: {LOCATION_PAGE_THRESHOLD}+ points",
        "=" * 80,
        "",
        f"Total Carriers Analyzed: {total_carriers}",
        f"Carriers with Location Pages: {carriers_with_data}",
        "",
        "MODALITY SUMMARY (carriers using each type):",
        "-" * 40,
    ]
    
    for modality, count in sorted(all_modalities.items(), key=lambda x: -x[1]):
        lines.append(f"  {modality}: {count} carriers")
    
    lines.extend([
        "",
        "=" * 80,
        "CARRIER DETAILS",
        "=" * 80,
    ])
    
    for carrier_name, report in sorted(reports.items()):
        lines.extend([
            "",
            f"### {carrier_name} ({report.domain})",
            f"Total pages: {report.total_pages}",
            f"Location pages (score >= {LOCATION_PAGE_THRESHOLD}): {report.location_pages}",
        ])
        
        if report.modalities_found:
            lines.append(f"Modalities: {dict(report.modalities_found)}")
        else:
            lines.append("Modalities: None detected")
        
        lines.append(f"Approach: {report.recommended_approach}")
        
        if report.top_pages:
            lines.append("Top location pages:")
            for page in report.top_pages[:5]:
                signals_str = ', '.join(f"{s.signal_type}({s.points})" for s in page.signals if s.points > 0)
                title_short = (page.title[:40] + '...') if len(page.title) > 40 else page.title
                lines.append(f"  - [{page.total_score}pts] {title_short or page.url[:40]}")
                lines.append(f"    Signals: {signals_str}")
    
    report_text = '\n'.join(lines)
    output_path.write_text(report_text)
    print(f"\nReport saved to: {output_path}")
    
    # JSON version
    json_path = output_path.with_suffix('.json')
    json_data = {
        carrier: {
            'carrier_name': r.carrier_name,
            'domain': r.domain,
            'total_pages': r.total_pages,
            'location_pages': r.location_pages,
            'modalities_found': r.modalities_found,
            'recommended_approach': r.recommended_approach,
            'top_pages': [
                {
                    'url': p.url,
                    'title': p.title,
                    'score': p.total_score,
                    'signals': [asdict(s) for s in p.signals if s.points > 0]
                }
                for p in r.top_pages[:10]
            ]
        }
        for carrier, r in reports.items()
    }
    json_path.write_text(json.dumps(json_data, indent=2))
    print(f"JSON saved to: {json_path}")


if __name__ == '__main__':
    data_dir = Path(__file__).parent / 'data'
    
    print("=" * 60)
    print("LOCATION DATA CLASSIFIER v2")
    print(f"Score threshold: {LOCATION_PAGE_THRESHOLD}+ points")
    print("=" * 60)
    
    reports = classify_all_carriers(data_dir)
    
    if reports:
        output_path = data_dir / 'reports' / 'modality_report.txt'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        generate_modality_report(reports, output_path)
    else:
        print("No carriers to classify.")
