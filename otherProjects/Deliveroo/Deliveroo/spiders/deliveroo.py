import copy
import csv
import json

import scrapy


class DeliverooSpider(scrapy.Spider):
    name = 'deliveroo'
    request_api = 'https://api.ae.deliveroo.com/consumer/graphql/'
    base_url = 'https://deliveroo.ae{}'
    custom_settings = {
        'FEED_URI': 'deliveroo.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    payload = {
        "query": "\n      query getHomeFeed(\n        $ui_actions: [UIActionType!]\n        $ui_blocks: [UIBlockType!]\n        $ui_controls: [UIControlType!]\n        $ui_features: [UIFeatureType!]\n        $ui_layouts: [UILayoutType!]\n        $ui_layout_carousel_styles: [UILayoutCarouselStyle!]\n        $ui_lines: [UILineType!]\n        $ui_targets: [UITargetType!]\n        $ui_themes: [UIThemeType!]\n        $fulfillment_methods: [FulfillmentMethod!]\n        $location: LocationInput!\n        $url: String\n        $options: SearchOptionsInput\n        $uuid: String!\n      ) {\n        results: search(\n          location: $location\n          options: $options\n          url: $url\n          capabilities: {\n            ui_actions: $ui_actions,\n            ui_blocks: $ui_blocks,\n            ui_controls: $ui_controls,\n            ui_features: $ui_features,\n            ui_layouts: $ui_layouts,\n            ui_layout_carousel_styles: $ui_layout_carousel_styles,\n            ui_lines: $ui_lines\n            ui_targets: $ui_targets,\n            ui_themes: $ui_themes,\n            fulfillment_methods: $fulfillment_methods\n          }\n          uuid: $uuid\n        ) {\n          layoutGroups: ui_layout_groups {\n            id\n            subheader\n            data: ui_layouts { ...uiHomeLayoutFields }\n          }\n\n          controlGroups: ui_control_groups {\n            appliedFilters: applied_filters { ...uiControlAppliedFilterFields }\n            filters { ...uiControlFilterFields }\n            sort { ...uiControlFilterFields }\n            queryResults: query_results { ...uiControlQueryResultFields }\n            fulfillmentMethods: fulfillment_methods { ...uiControlFulfillmentMethodFields }\n          }\n\n          modals: ui_modals {\n            ...uiModalFields\n            ...uiChallengesModalFields\n            ...uiPlusFullScreenModalFields\n          }\n\n          overlays: ui_feed_overlays {\n            ...uiFeedOverlayFields\n          }\n\n          meta {\n            ...searchResultMetaFields\n          }\n        }\n\n        \n  fulfillmentTimes: fulfillment_times(\n    capabilities: {\n      ui_actions: $ui_actions,\n      ui_blocks: $ui_blocks,\n      ui_controls: $ui_controls,\n      ui_features: $ui_features,\n      ui_layouts: $ui_layouts,\n      ui_layout_carousel_styles: $ui_layout_carousel_styles,\n      ui_lines: $ui_lines\n      ui_targets: $ui_targets,\n      ui_themes: $ui_themes,\n      fulfillment_methods: $fulfillment_methods\n    }\n    fulfillment_methods: $fulfillment_methods\n    location: $location\n    uuid: $uuid\n  ) {\n    fulfillmentTimeMethods: fulfillment_time_methods {\n      fulfillmentMethodLabel: fulfillment_method_label\n      fulfillmentMethod: fulfillment_method\n      asap {\n        ...fulfillmentTimeOptionFields\n      }\n      days {\n        day\n        dayLabel: day_label\n        times {\n          ...fulfillmentTimeOptionFields\n        }\n      }\n    }\n  }\n\n      }\n      \n  fragment fulfillmentTimeOptionFields on FulfillmentTimeOption {\n    optionLabel: option_label\n    selectedLabel: selected_label\n    timestamp(format: UNIX)\n    selectedTime: selected_time {\n      day\n      time\n    }\n  }\n\n      \n  fragment uiControlFulfillmentMethodFields on UIControlFulfillmentMethod {\n    label\n    targetMethod: target_method\n  }\n\n      \n  fragment searchResultMetaFields on SearchResultMeta {\n    location {\n      cityName: city_name\n      cityUname: city_uname\n      isoCode: country_iso_code\n      countryName: country_name\n      geohash\n      lat\n      lon\n      neighborhoodUname: neighborhood_uname\n      neighborhoodName: neighborhood_name\n      postcode\n    }\n    options {\n      fulfillmentMethod: fulfillment_method\n      deliveryTime: delivery_time\n      params {\n        id\n        value\n      }\n      query\n    }\n    restaurantCount: restaurant_count {\n      results\n      location\n    }\n    searchPlaceholder: search_placeholder\n    title\n    collection {\n      linkTitle: link_title\n      targetParams: target_params {\n        ...uiTargetParamsFields\n      }\n      previousTargetParams: previous_target_params {\n        ...uiTargetParamsFields\n      }\n      searchBarMeta: search_bar_meta {\n        searchBarPlaceholder: search_bar_placeholder\n        searchBarParams: search_bar_params {\n          id\n          value\n        }\n      }\n    }\n    uuid\n    web {\n      url\n    }\n    warnings {\n      type: warning_type\n    }\n    searchPills: search_pills {\n      id\n      label\n      placeholder\n      params {\n        id\n        value\n      }\n    }\n  }\n\n      \n  fragment uiTargetFields on UITarget {\n    typeName: __typename\n    ... on UITargetRestaurant {\n      ...uiTargetRestaurant\n    }\n    ... on UITargetParams {\n      ...uiTargetParamsFields\n    }\n    ... on UITargetAction {\n      action\n    }\n    ... on UITargetMenuItem {\n      ...uiTargetMenuItem\n    }\n    ... on UITargetDeepLink {\n      webTarget: fallback_target {\n        uri: url\n      }\n    }\n    ... on UITargetMenuItemModifier {\n      ...uiTargetMenuItemModifier\n    }\n  }\n\n      \n  fragment uiBlockFields on UIBlock {\n    typeName: __typename\n    ... on UIBanner {\n      key\n      header\n      caption\n      backgroundColor: background_color {\n        ...colorFields\n      }\n      buttonCaption: button_caption\n      contentDescription: content_description\n      target {\n        ...uiTargetFields\n      }\n      images {\n        icon {\n          ...iconFields\n        }\n        image\n      }\n      theme: ui_theme\n      trackingId: tracking_id\n      trackingProperties: tracking_properties\n    }\n    ... on UIButton {\n      key\n      text\n      contentDescription: content_description\n      target {\n        ...uiTargetFields\n      }\n      theme: ui_theme\n      trackingId: tracking_id\n      trackingProperties: tracking_properties\n    }\n    ... on UIShortcut {\n      key\n      images {\n        default\n      }\n      name\n      contentDescription: content_description\n      nameColor: name_color {\n        ...colorFields\n      }\n      backgroundColor: background_color {\n        ...colorFields\n      }\n      target {\n        ...uiTargetFields\n      }\n      theme: ui_theme\n      trackingId: tracking_id\n    }\n    ... on UICard {\n      key\n      trackingId: tracking_id\n      trackingProperties: tracking_properties\n      theme: ui_theme\n      contentDescription: content_description\n      border {\n        ...uiCardBorderFields\n      }\n      target {\n        ...uiTargetFields\n      }\n      uiContent: properties {\n        default {\n          ...uiHomeCardFields\n        }\n        expanded {\n          ...uiHomeCardFields\n        }\n      }\n    }\n    ... on UIMerchandisingCard {\n      key\n      headerImageUrl: header_image_url\n      backgroundImageUrl: background_image_url\n      contentDescription: content_description\n      uiLines: ui_lines {\n        ...uiLines\n      }\n      cardBackgroundColor: background_color {\n        ...colorFields\n      }\n      buttonCaption: button_caption\n      target {\n        ...uiTargetFields\n      }\n      trackingId: tracking_id\n    }\n    ... on UICategoryPill {\n      ...uiCategoryPillFields\n    }\n    ... on UITallMenuItemCard {\n      ...uiTallMenuItemCardFields\n    }\n    ... on UIStoryCard {\n      ...uiStoryCardFields\n    }\n  }\n\n      \n  fragment uiHomeLayoutFields on UILayout {\n    typeName: __typename\n    ... on UILayoutCarousel {\n      header\n      subheader\n      style\n      imageUrl: image_url\n      target {\n        ...uiTargetFields\n      }\n      uiLines: ui_lines {\n        ...uiLines\n      }\n      targetPresentational: target_presentational\n      key\n      blocks: ui_blocks {\n        ...uiBlockFields\n      }\n      trackingId: tracking_id\n      rows\n    }\n    ... on UILayoutList {\n      header\n      key\n      blocks: ui_blocks {\n        ...uiBlockFields\n      }\n      trackingId: tracking_id\n    }\n  }\n\n      \n  fragment uiControlFilterFields on UIControlFilter {\n    id\n    header\n    images {\n      icon {\n        name\n        image\n      }\n    }\n    optionsType: options_type\n    options {\n      count\n      default\n      name: header\n      id\n      selected\n      target_params {\n        ...uiTargetParamsFields\n      }\n    }\n    styling {\n      web {\n        desktop {\n          collapse\n        }\n        mobile {\n          collapse\n        }\n      }\n    }\n  }\n\n      \n  fragment uiControlAppliedFilterFields on UIControlAppliedFilter {\n    label\n    target_params {\n      ...uiTargetParamsFields\n    }\n  }\n\n      \n  fragment uiTargetParamsFields on UITargetParams {\n    params {\n      id\n      value\n    }\n    queryParams: query_params\n    title\n    typeName: __typename\n  }\n\n      \n  fragment uiTargetRestaurant on UITargetRestaurant {\n    restaurant {\n      id\n      name\n      links {\n        self {\n          href\n        }\n      }\n    }\n    typeName: __typename\n  }\n\n      \n  fragment uiTargetMenuItem on UITargetMenuItem {\n    menuItem: menu_item {\n      id\n    }\n    links {\n      href\n    }\n    typeName: __typename\n  }\n\n      \n  fragment uiTargetMenuItemModifier on UITargetMenuItemModifier {\n    restaurantId: restaurant_id\n    menuItemId: menu_item_id\n    uiTargetType: ui_target_type\n  }\n\n      \n  fragment uiControlQueryResultFields on UIControlQueryResult {\n    header\n    key\n    resultTarget: result_target {\n      ...uiTargetFields\n    }\n    targetPresentational: result_target_presentational\n    trackingId: tracking_id\n    options {\n      key\n      count\n      highlights {\n        begin\n        end\n      }\n      uiLines: ui_lines {\n        ...uiLines\n      }\n      image {\n        type: __typename\n        ... on DeliverooIcon {\n          ...iconFields\n        }\n        ... on UIControlQueryResultOptionImageSet {\n          default\n        }\n      }\n      label\n      isAvailable: is_available\n      target {\n        ...uiTargetFields\n      }\n      trackingId: tracking_id\n    }\n  }\n\n      \n  fragment colorFields on Color {\n    hex\n    r: red\n    g: green\n    b: blue\n    a: alpha\n  }\n\n      \n  fragment colorGradientFields on ColorGradient {\n    from {\n      ...colorFields\n    }\n    to {\n      ...colorFields\n    }\n  }\n\n      \n  fragment iconFields on DeliverooIcon {\n    name\n    image\n  }\n\n      \n  fragment illustrationBadgeFields on DeliverooIllustrationBadge {\n    name\n    image\n  }\n\n      \n  fragment uiHomeCardFields on UICardFields {\n    bubble {\n      uiLines: ui_lines {\n        ...uiLines\n      }\n    }\n    overlay {\n      background: background {\n        typeName: __typename\n        ...colorFields\n        ...colorGradientFields\n      }\n      text {\n        position\n        color {\n          ...colorFields\n        }\n        value\n      }\n      promotionTag: promotion_tag {\n        primaryTagLine: primary_tag_line {\n          backgroundColor: background_color {\n            ...colorFields\n            ...colorGradientFields\n          }\n          text {\n            ...uiLines\n          }\n        }\n        secondaryTagLine: secondary_tag_line {\n          backgroundColor: background_color {\n            ...colorFields\n            ...colorGradientFields\n          }\n          text {\n            ...uiLines\n          }\n        }\n      }\n    }\n    favouritesOverlay: favourites_overlay {\n      id\n      entity\n      isSelected: is_selected\n      backgroundColor: background_color {\n        ...colorFields\n        ...colorGradientFields\n      }\n      selectedColor: selected_color {\n        ...colorFields\n      }\n      unselectedColor: unselected_color {\n        ...colorFields\n      }\n      target {\n        ...uiTargetFields\n      }\n      countData: count_data {\n        count\n        isMaxCount: is_max_count\n      }\n    }\n    countdownBadgeOverlay: countdown_badge_overlay {\n      backgroundColor: background_color {\n        ...colorFields\n      }\n      uiLine: ui_line {\n        ...uiLines\n      }\n    }\n    image\n    uiLines: ui_lines {\n      ...uiLines\n    }\n  }\n\n      \n  fragment uiTextLine on UITextLine {\n    typeName: __typename\n    key\n    uiSpans: ui_spans {\n      ...uiSpansPrimitive\n      ... on UISpanCountdown {\n        endsAt: ends_at\n        isBold: is_bold\n        size\n        key\n        color {\n          ...colorFields\n        }\n      }\n      ... on UISpanTag {\n        key\n        uiSpans: ui_spans {\n          ...uiSpansPrimitive\n        }\n        backgroundColor: background_color {\n          ...colorFields\n        }\n      }\n    }\n  }\n\n      \n  fragment uiLines on UILine {\n    typeName: __typename\n    ... on UITextLine {\n      ...uiTextLine\n    }\n    ... on UITitleLine {\n      key\n      text\n      color {\n        ...colorFields\n      }\n      size\n    }\n    ... on UIBulletLine {\n      key\n      iconSpan: icon_span {\n        typeName: __typename\n        color {\n          ...colorFields\n        }\n        icon {\n          ...iconFields\n        }\n        iconSize: size\n      }\n      bulletSpacerSpan: bullet_spacer_span {\n        typeName: __typename\n        width\n      }\n      uiSpans: ui_spans {\n        ...uiSpansPrimitive\n      }\n    }\n  }\n\n      \n  fragment uiSpansPrimitive on UISpan {\n    typeName: __typename\n    ... on UISpanIcon {\n      key\n      color {\n        ...colorFields\n      }\n      icon {\n        ...iconFields\n      }\n      iconSize: size\n    }\n    ... on UISpanSpacer {\n      key\n      width\n    }\n    ... on UISpanText {\n      key\n      color {\n        ...colorFields\n      }\n      text\n      isBold: is_bold\n      textSize: size\n    }\n  }\n\n      \n  fragment uiCardBorderFields on UICardBorderType {\n    topColor: top_color {\n      ...colorFields\n    }\n    bottomColor: bottom_color {\n      ...colorFields\n    }\n    leftColor: left_color {\n      ...colorFields\n    }\n    rightColor: right_color {\n      ...colorFields\n    }\n    borderWidth: border_width\n  }\n\n      \n  fragment uiModalButtonFields on UIModalButton {\n    title\n    theme: ui_theme\n    dismissOnAction: dismiss_on_action\n    target {\n      typeName: __typename\n      ... on UITargetWebPage {\n        url\n        newWindow: new_window\n      }\n      ... on UITargetAction {\n        action\n        params {\n          id\n          value\n        }\n      }\n      ... on UITargetParams {\n        ...uiTargetParamsFields\n      }\n    }\n    trackingId: tracking_id\n  }\n\n      \n  fragment uiModalFields on UIModal {\n    typeName: __typename\n    header\n    caption\n    image {\n      ... on UIModalImage {\n        image\n      }\n      ... on DeliverooIcon {\n        ...iconFields\n      }\n      ... on DeliverooIllustrationBadge {\n        ...illustrationBadgeFields\n      }\n    }\n    buttons {\n      ...uiModalButtonFields\n    }\n    theme: ui_theme\n    displayId: display_id\n    trackingId: tracking_id\n  }\n\n      \n  fragment uiChallengesModalFields on UIChallengesModal {\n    typeName: __typename\n    displayId: display_id\n    trackingId: tracking_id\n    challengeDrnId: challenges_drn_id\n    mode\n    smallView: small_view {\n      header\n      bodyText: body_text\n      infoButton: info_button {\n        ...uiModalButtonFields\n      }\n      icon {\n        ... on UIChallengesIndicator {\n          required\n          completed\n        }\n        ... on UIChallengesBadge {\n          url\n        }\n        ... on UIChallengesSteppedIndicator {\n          steps {\n            ... on UIChallengesSteppedStamp {\n              text\n              icon\n              isHighlighted: is_highlighted\n            }\n          }\n          stepsCompleted: steps_completed\n          stepsRequired: steps_required\n        }\n      }\n    }\n    fullView: full_view {\n      header\n      headerSubtitle: header_subtitle\n      bodyTitle: body_title\n      bodyText: body_text\n      confirmationButton: confirmation_button {\n        ...uiModalButtonFields\n      }\n      infoButton: info_button {\n        ...uiModalButtonFields\n      }\n      icon {\n        ... on UIChallengesIndicator {\n          required\n          completed\n        }\n        ... on UIChallengesBadge {\n          url\n        }\n        ... on UIChallengesSteppedIndicator {\n          steps {\n            ... on UIChallengesSteppedStamp {\n              text\n              icon\n              isHighlighted: is_highlighted\n            }\n          }\n          stepsCompleted: steps_completed\n          stepsRequired: steps_required\n        }\n      }\n    }\n  }\n\n      \n  fragment uiPlusFullScreenModalFields on UIPlusFullScreenModal {\n    typeName: __typename\n    displayId: display_id\n    trackingId: tracking_id\n    image {\n      typeName: __typename\n      ... on UIModalImage {\n        image\n      }\n      ... on DeliverooIllustrationBadge {\n        ...illustrationBadgeFields\n      }\n    }\n    header\n    body\n    footnote\n    primaryButton {\n      ...uiModalButtonFields\n    }\n    secondaryButton {\n      ...uiModalButtonFields\n    }\n    confetti\n    displayOnlyOnce: display_only_once\n  }\n\n      \n  fragment uiFeedOverlayFields on UIFeedOverlay {\n    id\n    position\n    blocks: overlay_blocks {\n      ... on UIFeedOverlayBanner {\n        typeName: __typename\n        id: display_id\n        trackingId: tracking_id\n        header\n        caption\n        isDismissible: is_dismissible\n        theme: ui_theme\n        image {\n          ... on DeliverooIllustrationBadge {\n            name\n          }\n        }\n      }\n    }\n  }\n\n      \n  fragment uiCategoryPillFields on UICategoryPill {\n    typeName: __typename\n    blocks: content {\n      ...uiLines\n    }\n    backgroundColor: background_color {\n      typeName: __typename\n      ...colorFields\n    }\n    target {\n      ...uiTargetFields\n    }\n    trackingId: tracking_id\n    contentDescription: content_description\n  }\n\n      \n  fragment uiTallMenuItemCardFields on UITallMenuItemCard {\n    id: menu_item_id\n    title\n    key\n    image\n    target {\n      ...uiTargetFields\n    }\n    price {\n      ...currencyFields\n    }\n    trackingId: tracking_id\n  }\n\n      \n  fragment currencyFields on Currency {\n    code\n    formatted\n    fractional\n    presentational\n  }\n\n      \n  fragment uiStoryCardFields on UIStoryCard {\n    preview {\n      profile {\n        imageUrl: image_url\n        headingLines: heading_lines {\n          ...uiLines\n        }\n      }\n      video {\n        sources {\n          url\n          type\n        }\n        placeholderImage: placeholder_url\n        autoplay\n        trackingId: tracking_id\n      }\n      overlay {\n        typeName: __typename\n        ... on UIStoryTextOverlay {\n          background {\n            typeName: __typename\n            ...colorFields\n            ...colorGradientFields\n          }\n          uiLines: ui_lines {\n            ...uiLines\n          }\n        }\n      }\n      target {\n        ...uiTargetFields\n      }\n    }\n    main {\n      profile {\n        imageUrl: image_url\n        headingLines: heading_lines {\n          ...uiLines\n        }\n      }\n      video {\n        sources {\n          url\n          type\n        }\n        placeholderImage: placeholder_url\n        autoplay\n        trackingId: tracking_id\n      }\n      overlay {\n        typeName: __typename\n        ... on UIStoryButtonOverlay {\n          background {\n            typeName: __typename\n            ...colorFields\n            ...colorGradientFields\n          }\n          contentLines: content {\n            ...uiLines\n          }\n          button {\n            key\n            text\n            contentDescription: content_description\n            target {\n              ...uiTargetFields\n            }\n            theme: ui_theme\n            trackingId: tracking_id\n            trackingProperties: tracking_properties\n          }\n        }\n      }\n    }\n    trackingId: tracking_id\n    trackingProperties: tracking_properties\n    key\n  }\n\n    ",
        "variables": {
            "ui_blocks": [
                "BANNER",
                "CARD",
                "SHORTCUT",
                "BUTTON",
                "MERCHANDISING_CARD",
                "STORY_CARD"
            ],
            "ui_controls": [
                "APPLIED_FILTER",
                "FILTER",
                "SORT"
            ],
            "ui_layout_carousel_styles": [
                "DEFAULT",
                "PARTNER_HEADING"
            ],
            "ui_lines": [
                "TITLE",
                "TEXT",
                "BULLET"
            ],
            "ui_targets": [
                "PARAMS",
                "RESTAURANT",
                "MENU_ITEM",
                "WEB_PAGE",
                "DEEP_LINK"
            ],
            "fulfillment_methods": [
                "DELIVERY",
                "COLLECTION"
            ],
            "location": {
                "geohash": "thx2jnhkj1f6",
                "city_uname": "sharjah",
                "neighborhood_uname": "al-abar",
                "postcode": ""
            },
            "options": {
                "query": "",
                "web_column_count": 4,
                "user_preference": {
                    "seen_modals": [
                        {
                            "id": "nc_promos_voucher_january50%",
                            "timestamp": 1674490676
                        }
                    ]
                }
            },
            "url": "https://deliveroo.ae/restaurants/dubai/dubai-business-bay?geohash=thrr3squys6w&collection=all-restaurants",
            "uuid": "cb0505b7-0ab6-4f24-9c84-76697a080304",
            "ui_actions": [
                "CHANGE_DELIVERY_TIME",
                "CLEAR_FILTERS",
                "NO_DELIVERY_YET",
                "SHOW_MEAL_CARD_ISSUERS",
                "SHOWCASE_PICKUP",
                "TOGGLE_FAVOURITE",
                "COPY_TO_CLIPBOARD",
                "SHOW_PICKUP",
                "SHOW_DELIVERY",
                "REFRESH",
                "SHOW_VIDEO_STORIES",
                "SHOW_HOME_MAP_VIEW",
                "ACCEPT_CHALLENGES",
                "SHOW_CHALLENGES_DETAILS"
            ],
            "ui_features": [
                "UNAVAILABLE_RESTAURANTS",
                "LIMIT_QUERY_RESULTS",
                "UI_CARD_BORDER",
                "UI_CAROUSEL_COLOR",
                "UI_PROMOTION_TAG",
                "UI_BACKGROUND",
                "ILLUSTRATION_BADGES",
                "SCHEDULED_RANGES",
                "UI_SPAN_TAGS",
                "UI_SPAN_COUNTDOWN",
                "HOME_MAP_VIEW"
            ],
            "ui_themes": [
                "BANNER_CARD",
                "BANNER_EMPTY",
                "BANNER_MARKETING_A",
                "BANNER_MARKETING_B",
                "BANNER_MARKETING_C",
                "BANNER_PICKUP_SHOWCASE",
                "BANNER_SERVICE_ADVISORY",
                "CARD_LARGE",
                "CARD_MEDIUM",
                "CARD_MEDIUM_HORIZONTAL",
                "CARD_SMALL",
                "CARD_SMALL_DIAGONAL",
                "CARD_SMALL_HORIZONTAL",
                "CARD_WIDE",
                "CARD_TALL",
                "CARD_TALL_GRADIENT",
                "MODAL_DEFAULT",
                "MODAL_PLUS",
                "MODAL_BUTTON_PRIMARY",
                "MODAL_BUTTON_SECONDARY",
                "MODAL_BUTTON_TERTIARY",
                "SHORTCUT_DEFAULT",
                "SHORTCUT_STACKED",
                "SHORTCUT_HORIZONTAL",
                "BUTTON_PRIMARY",
                "BUTTON_SECONDARY",
                "ANY_MODAL"
            ],
            "ui_layouts": [
                "LIST",
                "CAROUSEL"
            ]
        }
    }
    headers = {
        'authority': 'api.ae.deliveroo.com',
        'accept': 'application/json, application/vnd.api+json',
        'accept-language': 'en',
        'authorization': '',
        'content-type': 'application/json',
        'origin': 'https://deliveroo.ae',
        'referer': 'https://deliveroo.ae/',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'x-roo-client': 'consumer-web-app',
        'x-roo-client-referer': 'https://deliveroo.ae/',
        'x-roo-country': 'ae',
        'x-roo-external-device-id': '',
        'x-roo-guid': 'ee3103d0-af23-4289-8c28-61108eb88285',
        'x-roo-session-guid': '4664d989-efe7-447c-b61a-b157d3901422',
        'x-roo-sticky-guid': 'ee3103d0-af23-4289-8c28-61108eb88285',
        'Cookie': '__cf_bm=reGseRN1nZLH2BoMsf70ickFrfWm1v0zxKUp.ota0Qk-1674491140-0-Afa9bw48ARAW5l78aKli6itkK4wQpyARi/ABwFPdSg6BcYQeeUHNQm/bCyZTsoEna/azpcuVH1VtLxFYZ05pQSYTaygDaH7T4dgoWDbfOEQi'
    }
    detail_headers = {
        'authority': 'deliveroo.ae',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_url = self.get_search_url()

    def get_search_url(self):
        with open('uae_cities.csv', 'r', encoding='utf-8-sig') as reader:
            return list(csv.DictReader(reader))

    def start_requests(self):
        for urls in self.request_url:
            payload = copy.deepcopy(self.payload)
            payload['variables']['url'] = urls.get('urls')
            yield scrapy.Request(url=self.request_api, callback=self.parse, method='POST',
                                 body=json.dumps(payload), headers=self.headers)

    def parse(self, response):
        json_data = json.loads(response.body)
        restaurant_details = json_data.get('data', {}).get('results', {}).get('layoutGroups', [])
        if restaurant_details:
            for data in restaurant_details[0].get('data', []):
                item = dict()
                for restaurant in data.get('blocks', []):
                    item['Name'] = restaurant.get('target', {}).get('restaurant', {}).get('name', '')
                    item['Content_Description'] = restaurant.get('contentDescription', '')
                    restaurant_link = restaurant.get('target', {}).get('restaurant', {}).get('links', {}).get('self',
                                                                                                              {}).get(
                        'href', '')
                    if restaurant_link:
                        yield scrapy.Request(url=self.base_url.format(restaurant_link), callback=self.parse_details,
                                             headers=self.detail_headers, meta={'item': item})

    def parse_details(self, response):
        json_data = response.css('script[id="__NEXT_DATA__"]::text').get()
        if json_data:
            load_json = json.loads(json_data)
            props = load_json.get('props', {}).get('initialState', {}).get('menuPage', {}).get('menu', {}).get('meta',
                                                                                                               {})
            for data in props.get('items', []):
                item = response.meta['item']
                item['Restaurant Id'] = props.get('restaurant', {}).get('id', '')
                item['image_url'] = props.get('metatags', {}).get('image', '')
                item['description'] = props.get('metatags', {}).get('description', '')
                item['City Id'] = props.get('restaurant', {}).get('location', {}).get('cityId', '')
                item['Zone Id'] = props.get('restaurant', {}).get('location', {}).get('zoneId', '')
                item['Address 1'] = props.get('restaurant', {}).get('location', {}).get('address', {}).get('address1',
                                                                                                           '')
                item['Neighborhood'] = props.get('restaurant', {}).get('location', {}).get('address', {}).get(
                    'neighborhood', '')
                item['City'] = props.get('restaurant', {}).get('location', {}).get('address', {}).get('city', '')
                item['Zip Code'] = props.get('restaurant', {}).get('location', {}).get('address', {}).get('postCode',
                                                                                                          '')
                item['Country'] = props.get('restaurant', {}).get('location', {}).get('address', {}).get('country', '')
                item['Menu Item Name'] = data.get('name', '')
                item['Menu Item Description'] = data.get('description', '')
                item['Menu Item Price'] = data.get('price', {}).get('formatted', '')
                category_id = data.get('categoryId', '')
                Categories = props.get('categories', {})
                matched = [d for d in Categories if d['id'] == category_id]
                if matched:
                    item['Categories'] = matched[0].get('name', '')
                item['Detail_url'] = response.url
                item['Menu Item Url'] = response.url + '&item-id=' + data.get('id', '')
                yield item
